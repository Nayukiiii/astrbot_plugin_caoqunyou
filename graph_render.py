"""
草群友关系图 —— 纯 Pillow 渲染
始终保持节点+有向箭头的关系图形态，通过边密度自适应裁剪避免糊成毛线球。

节点布局：
  ≤15 节点 → 力导向
  >15 节点 → 同心圆（草出次数越多越靠近中心）

边裁剪策略（按节点数自动调整）：
  ≤15  → 全部边
  ≤40  → 只保留互草边 + 单向草出≥2次的边
  >40  → 只保留互草边 + 单向草出≥3次的边，总边数上限200条
"""
from __future__ import annotations
from PIL import Image, ImageDraw, ImageFont
import os, io, math, asyncio, random, aiohttp
from collections import defaultdict

_FONT_REG  = "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"
_FONT_BOLD = "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc"
if __import__("sys").platform == "win32":
    _WIN = os.path.join(os.environ.get("WINDIR","C:\\Windows"),"Fonts")
    for _v,_cs in [("_FONT_REG",["msyh.ttc","simsun.ttc"]),("_FONT_BOLD",["msyhbd.ttc","msyh.ttc"])]:
        for _c in _cs:
            _p = os.path.join(_WIN,_c)
            if os.path.exists(_p): globals()[_v]=_p; break

def _font(p,s):
    try:    return ImageFont.truetype(p,s)
    except: return ImageFont.load_default()
def _tw(d,t,f): bb=d.textbbox((0,0),t,font=f); return bb[2]-bb[0]
def _th(d,t,f): bb=d.textbbox((0,0),t,font=f); return bb[3]-bb[1]
def _tc(d,cx,y,t,f,c): d.text((cx-_tw(d,t,f)//2,y),t,font=f,fill=c)
def _grad_h(d,x0,y0,x1,y1,cl,cr):
    w=x1-x0
    if w<=0: return
    for i in range(w):
        t=i/(w-1) if w>1 else 0
        c=tuple(int(cl[k]+(cr[k]-cl[k])*t) for k in range(3))
        d.line([(x0+i,y0),(x0+i,y1)],fill=c)
def _grad_v(d,x0,y0,x1,y1,ct,cb):
    h=y1-y0
    if h<=0: return
    for i in range(h):
        t=i/(h-1) if h>1 else 0
        c=tuple(int(ct[k]+(cb[k]-ct[k])*t) for k in range(3))
        d.line([(x0,y0+i),(x1,y0+i)],fill=c)

# ── 色彩 ─────────────────────────────────────────────────────────
BG_TOP=(255,242,247); BG_BTM=(255,220,235)
HDR_L=(255,183,197);  HDR_R=(200,90,124)
NODE_BORDER=(200,90,124); NODE_FILL=(255,245,248); NODE_SHADOW=(240,195,210)
EDGE_SINGLE=(200,90,124); EDGE_MUTUAL=(130,60,180)
LABEL_BG=(255,255,255)
TEXT_MAIN=(50,38,52); TEXT_LIGHT=(255,255,255); TEXT_GRAY=(150,130,145)
RING_COLORS=[(200,90,124),(150,80,160),(60,140,200),(40,160,120),(180,130,40)]
FOOT_FG=(200,90,124)


# ── 头像下载 ─────────────────────────────────────────────────────
async def _fetch_avatar(qq:str, cache_dir:str) -> bytes|None:
    path = os.path.join(cache_dir, f"avatar_{qq}.jpg")
    if os.path.exists(path):
        try:
            with open(path,"rb") as f: return f.read()
        except: pass
    url = f"https://q4.qlogo.cn/headimg_dl?dst_uin={qq}&spec=100"
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=5)) as r:
                if r.status == 200:
                    data = await r.read()
                    os.makedirs(cache_dir, exist_ok=True)
                    with open(path,"wb") as f: f.write(data)
                    return data
    except: pass
    return None


# ── 力导向布局 ────────────────────────────────────────────────────
def _fr_layout(nodes, edges, W, H, iterations=120, seed=42):
    rng = random.Random(seed); n = len(nodes)
    if n == 0: return {}
    if n == 1: return {nodes[0]: (W/2, H/2)}
    PAD = 120; area = (W-PAD*2)*(H-PAD*2); k = math.sqrt(area/n)*0.9
    pos = {nd: [W/2 + min(W,H)*0.28*math.cos(2*math.pi*i/n) + rng.uniform(-5,5),
                H/2 + min(W,H)*0.28*math.sin(2*math.pi*i/n) + rng.uniform(-5,5)]
           for i,nd in enumerate(nodes)}
    def rep(d): return k*k/(d+0.01)
    def att(d): return d*d/k
    for it in range(iterations):
        t = W*(1-it/iterations)*0.15
        disp = {nd:[0.,0.] for nd in nodes}
        for i,u in enumerate(nodes):
            for j,v in enumerate(nodes):
                if i>=j: continue
                dx=pos[u][0]-pos[v][0]; dy=pos[u][1]-pos[v][1]
                d=math.sqrt(dx*dx+dy*dy)+0.01; f=rep(d)/d
                disp[u][0]+=dx*f; disp[u][1]+=dy*f
                disp[v][0]-=dx*f; disp[v][1]-=dy*f
        for a,b in edges:
            if a not in pos or b not in pos: continue
            dx=pos[a][0]-pos[b][0]; dy=pos[a][1]-pos[b][1]
            d=math.sqrt(dx*dx+dy*dy)+0.01; f=att(d)/d
            disp[a][0]-=dx*f; disp[a][1]-=dy*f
            disp[b][0]+=dx*f; disp[b][1]+=dy*f
        for nd in nodes:
            dx,dy=disp[nd]; d=math.sqrt(dx*dx+dy*dy)+0.01
            sc=min(d,t)/d
            pos[nd][0]=max(PAD,min(W-PAD, pos[nd][0]+dx*sc))
            pos[nd][1]=max(PAD,min(H-PAD, pos[nd][1]+dy*sc))
    return {nd:(pos[nd][0],pos[nd][1]) for nd in nodes}


# ── 同心圆布局 ────────────────────────────────────────────────────
def _ring_layout(nodes, out_deg, W, H, node_r):
    n = len(nodes)
    if n == 0: return {}
    sorted_nodes = sorted(nodes, key=lambda nd: out_deg.get(nd,0), reverse=True)
    cx, cy = W//2, H//2
    pos = {}; spacing = node_r*2+16; placed = 0; ri = 0
    while placed < n:
        if ri == 0: cap=1; r_ring=0
        else:
            r_ring = ri*spacing*2 + node_r*3
            cap = max(1, int(2*math.pi*r_ring/spacing))
        batch = sorted_nodes[placed:placed+cap]
        if r_ring == 0:
            pos[batch[0]] = (float(cx), float(cy))
        else:
            for j,nd in enumerate(batch):
                ang = 2*math.pi*j/len(batch) - math.pi/2
                pos[nd] = (cx + r_ring*math.cos(ang), cy + r_ring*math.sin(ang))
        placed += len(batch); ri += 1
    return pos


# ── 圆形头像 ─────────────────────────────────────────────────────
def _paste_circle(img, av_bytes, cx, cy, r, border, shadow, bw=3):
    draw = ImageDraw.Draw(img)
    draw.ellipse([cx-r+3, cy-r+3, cx+r+3, cy+r+3], fill=shadow)
    draw.ellipse([cx-r, cy-r, cx+r, cy+r], fill=NODE_FILL, outline=border, width=bw)
    if av_bytes:
        try:
            av = Image.open(io.BytesIO(av_bytes)).convert("RGBA")
            av = av.resize((r*2, r*2), Image.LANCZOS)
            mask = Image.new("L", (r*2, r*2), 0)
            ImageDraw.Draw(mask).ellipse([0,0,r*2-1,r*2-1], fill=255)
            img.paste(av, (cx-r, cy-r), mask)
        except: pass
    ImageDraw.Draw(img).ellipse([cx-r, cy-r, cx+r, cy+r], outline=border, width=bw)


# ── 箭头 ─────────────────────────────────────────────────────────
def _draw_arrow(draw, x1,y1, x2,y2, color, lw, node_r, offset=0., alpha=255):
    dx=x2-x1; dy=y2-y1
    dist=math.sqrt(dx*dx+dy*dy)
    if dist < 1: return
    ux=dx/dist; uy=dy/dist; nx=-uy; ny=ux
    AH=16; SP=node_r+4; EP=node_r+AH+3
    sx=x1+ux*SP+nx*offset; sy=y1+uy*SP+ny*offset
    ex=x2-ux*EP+nx*offset; ey=y2-uy*EP+ny*offset
    def _b(c): return tuple(int(c[k]+(255-c[k])*(1-alpha/255)) for k in range(3))
    fc = _b(color)
    draw.line([(sx,sy),(ex,ey)], fill=fc, width=lw)
    tip_x=x2-ux*(node_r+2)+nx*offset; tip_y=y2-uy*(node_r+2)+ny*offset
    AW=9
    lx=tip_x-ux*AH+nx*AW; ly=tip_y-uy*AH+ny*AW
    rx=tip_x-ux*AH-nx*AW; ry=tip_y-uy*AH-ny*AW
    draw.polygon([(tip_x,tip_y),(lx,ly),(rx,ry)], fill=fc)


# ── 边裁剪 ────────────────────────────────────────────────────────
def _filter_edges(
    edge_count: dict[tuple[str,str],int],
    n_nodes: int,
) -> dict[tuple[str,str],int]:
    """
    根据节点数裁剪要渲染的边，避免密集时变成毛线球。
    始终优先保留：互草边 > 高频单向边。
    """
    mutual = {(a,b) for (a,b) in edge_count if (b,a) in edge_count}

    if n_nodes <= 15:
        # 全部保留
        return dict(edge_count)

    if n_nodes <= 40:
        min_cnt = 2   # 单向边至少2次才画
        max_edges = 300
    else:
        min_cnt = 3   # 单向边至少3次才画
        max_edges = 200

    # 互草边全保留，单向边按阈值过滤
    kept = {}
    for (a,b),cnt in edge_count.items():
        is_mutual = (a,b) in mutual
        if is_mutual or cnt >= min_cnt:
            kept[(a,b)] = cnt

    # 若还是超过 max_edges，按频次降序截断
    if len(kept) > max_edges:
        kept = dict(sorted(kept.items(), key=lambda x: (
            1 if (x[0][0],x[0][1]) in mutual else 0,  # 互草优先
            x[1]  # 频次其次
        ), reverse=True)[:max_edges])

    return kept


# ── 主渲染 ────────────────────────────────────────────────────────
async def render_graph(
    records:    list[dict],
    user_map:   dict[str,str],
    group_name: str,
    out_path:   str,
    cache_dir:  str|None = None,
    scale:      int = 2,
) -> str:
    S = scale

    # ── 聚合 ─────────────────────────────────────────────────────
    node_names: dict[str,str] = {}
    edge_count_full: dict[tuple[str,str],int] = {}
    out_deg: dict[str,int] = defaultdict(int)
    for r in records:
        aid=r["attacker_id"]; tid=r["target_id"]
        node_names[aid] = user_map.get(aid) or r.get("attacker_name") or f"用户{aid}"
        node_names[tid] = user_map.get(tid) or r.get("target_name")  or f"用户{tid}"
        key = (aid,tid)
        edge_count_full[key] = edge_count_full.get(key,0)+1
        out_deg[aid] += 1

    nodes = list(node_names.keys())
    n     = len(nodes)

    # ── 边裁剪 ───────────────────────────────────────────────────
    edge_count = _filter_edges(edge_count_full, n)
    edges_for_layout = list(edge_count.keys())   # 用于力导向引力计算
    mutual = {(a,b) for (a,b) in edge_count if (b,a) in edge_count}

    # ── 画布尺寸 ─────────────────────────────────────────────────
    USE_RING = n > 15
    HDR_H = 64*S; FOOT_H = 40*S

    if USE_RING:
        NODE_R = max(16*S, min(28*S, int(170*S/max(n,1))))
        spacing = NODE_R*2+12*S
        placed=1; ri=1
        while placed < n:
            r_r = ri*spacing*2+NODE_R*3
            cap = max(1, int(2*math.pi*r_r/spacing))
            placed+=cap; ri+=1
        max_r = ri*spacing*2+NODE_R*4
        CW = max(1000*S, int(max_r*2)+NODE_R*6+80*S)
        CH = max(800*S,  int(max_r*2)+NODE_R*6+80*S)
    else:
        NODE_R = max(28*S, min(42*S, int(260*S/max(n,1))))
        CW = 1000*S
        CH = max(700, 500+n*30)*S

    GRAPH_H = CH - HDR_H - FOOT_H

    # ── 下载头像 ─────────────────────────────────────────────────
    _cd = cache_dir or os.path.join(os.path.dirname(out_path), "avatar_cache")
    tasks = {qq: _fetch_avatar(qq,_cd) for qq in nodes}
    avatars: dict[str,bytes|None] = {}
    if tasks:
        fetched = await asyncio.gather(*tasks.values(), return_exceptions=True)
        for qq,data in zip(tasks.keys(), fetched):
            avatars[qq] = data if isinstance(data,bytes) else None

    # ── 布局 ─────────────────────────────────────────────────────
    if USE_RING:
        raw_pos = _ring_layout(nodes, out_deg, CW, GRAPH_H, NODE_R)
        # 计算每个节点所在圈号
        node_ring: dict[str,int] = {}
        sorted_nodes = sorted(nodes, key=lambda nd: out_deg.get(nd,0), reverse=True)
        spacing = NODE_R*2+12*S; placed=0; ri=0
        while placed < n:
            if ri==0: cap=1
            else:
                r_r=ri*spacing*2+NODE_R*3
                cap=max(1,int(2*math.pi*r_r/spacing))
            for nd in sorted_nodes[placed:placed+cap]: node_ring[nd]=ri
            placed+=cap; ri+=1
    else:
        iters = max(80, min(200, 60+n*8))
        raw_pos = _fr_layout(nodes, edges_for_layout, CW, GRAPH_H, iterations=iters)
        node_ring = {nd:0 for nd in nodes}

    pos = {nd:(x, y+HDR_H) for nd,(x,y) in raw_pos.items()}

    # ── 画布 ─────────────────────────────────────────────────────
    img  = Image.new("RGB", (CW,CH), BG_TOP)
    draw = ImageDraw.Draw(img)
    _grad_v(draw, 0,0,CW,CH, BG_TOP,BG_BTM)

    # 同心圆装饰圈
    if USE_RING:
        max_ring = max(node_ring.values()) if node_ring else 0
        gcx,gcy  = CW//2, HDR_H+GRAPH_H//2
        spacing  = NODE_R*2+12*S
        for ri2 in range(1, max_ring+1):
            r_r = ri2*spacing*2+NODE_R*3
            draw.ellipse([gcx-r_r, gcy-r_r, gcx+r_r, gcy+r_r],
                         outline=(255,220,230), width=S)

    # ── Header ───────────────────────────────────────────────────
    _grad_h(draw, 0,0,CW,HDR_H, HDR_L,HDR_R)
    fn_hdr = _font(_FONT_BOLD, 22*S)
    txt = f"🔥 {group_name} 今日草群友图谱 🔥"
    _tc(draw, CW//2, (HDR_H-_th(draw,txt,fn_hdr))//2, txt, fn_hdr, TEXT_LIGHT)

    # 边数提示（密集时显示裁剪说明）
    total_edges = sum(edge_count_full.values())
    shown_edges = sum(edge_count.values())
    fn_sub = _font(_FONT_REG, 7*S)
    if shown_edges < total_edges:
        sub = f"仅显示主要连线（{shown_edges}/{total_edges} 条）"
    else:
        sub = f"共 {total_edges} 条草草记录"
    draw.text((CW - _tw(draw,sub,fn_sub) - 10*S,
               HDR_H - _th(draw,sub,fn_sub) - 6*S),
              sub, font=fn_sub, fill=(255,230,238))

    # ── 画边 ─────────────────────────────────────────────────────
    EDGE_W   = max(2, 2*S)
    fn_elbl  = _font(_FONT_REG, 7*S if USE_RING else 8*S)
    max_cnt  = max(edge_count.values()) if edge_count else 1
    show_lbl = n <= 30   # 节点超过30时省略边标签，减少视觉噪音

    for (aid,tid),cnt in edge_count.items():
        if aid not in pos or tid not in pos: continue
        ax,ay = pos[aid]; tx,ty = pos[tid]
        is_m  = (aid,tid) in mutual
        color = EDGE_MUTUAL if is_m else EDGE_SINGLE
        alpha = int(100 + 155*(cnt/max_cnt))
        off   = 12*S if is_m else 0.
        lw    = min(EDGE_W + cnt - 1, EDGE_W+5)
        _draw_arrow(draw, ax,ay,tx,ty, color, lw, NODE_R, off, alpha)

        if show_lbl and cnt > 1:
            mx=(ax+tx)/2; my=(ay+ty)/2
            lbl = f"×{cnt}"
            lw2=_tw(draw,lbl,fn_elbl); lh2=_th(draw,lbl,fn_elbl)
            PX,PY=3*S,2*S
            bx0,by0=int(mx)-lw2//2-PX, int(my)-lh2//2-PY
            bx1,by1=bx0+lw2+PX*2, by0+lh2+PY*2
            draw.rectangle([bx0,by0,bx1,by1], fill=LABEL_BG)
            draw.rectangle([bx0,by0,bx1,by1], outline=color, width=S)
            bb=draw.textbbox((0,0),lbl,font=fn_elbl)
            draw.text((bx0+PX-bb[0], by0+PY-bb[1]), lbl, font=fn_elbl, fill=color)
        elif show_lbl and not USE_RING:
            mx=(ax+tx)/2; my=(ay+ty)/2
            lbl="草了"
            lw2=_tw(draw,lbl,fn_elbl); lh2=_th(draw,lbl,fn_elbl)
            PX,PY=3*S,2*S
            bx0,by0=int(mx)-lw2//2-PX, int(my)-lh2//2-PY
            bx1,by1=bx0+lw2+PX*2, by0+lh2+PY*2
            draw.rectangle([bx0,by0,bx1,by1], fill=LABEL_BG)
            draw.rectangle([bx0,by0,bx1,by1], outline=color, width=S)
            bb=draw.textbbox((0,0),lbl,font=fn_elbl)
            draw.text((bx0+PX-bb[0], by0+PY-bb[1]), lbl, font=fn_elbl, fill=color)

    # ── 画节点 ───────────────────────────────────────────────────
    fn_name = _font(_FONT_BOLD, 7*S if USE_RING else 9*S)
    for nd in nodes:
        if nd not in pos: continue
        cx2,cy2 = int(pos[nd][0]), int(pos[nd][1])
        ring    = node_ring.get(nd,0)
        border  = RING_COLORS[min(ring, len(RING_COLORS)-1)]
        bw      = max(2, 3*S//2 + (2 if ring==0 else 0))
        _paste_circle(img, avatars.get(nd), cx2,cy2, NODE_R, border, NODE_SHADOW, bw)
        draw = ImageDraw.Draw(img)

        # 中心节点★
        if USE_RING and ring == 0:
            fn_cr = _font(_FONT_BOLD, 10*S)
            draw.text((cx2-NODE_R+2, cy2-NODE_R-12*S), "★", font=fn_cr, fill=RING_COLORS[0])

        # 名字标签
        name = node_names[nd]
        max_nw = NODE_R*2 + 8*S
        while _tw(draw,name,fn_name) > max_nw and len(name) > 1: name=name[:-1]
        if name != node_names[nd]: name=name[:-1]+"…"
        nw=_tw(draw,name,fn_name); nh=_th(draw,name,fn_name)
        PX2,PY2=4*S,2*S
        lx0=cx2-nw//2-PX2; ly0=cy2+NODE_R+3*S
        lx1=lx0+nw+PX2*2;  ly1=ly0+nh+PY2*2
        draw.rectangle([lx0,ly0,lx1,ly1], fill=LABEL_BG)
        draw.rectangle([lx0,ly0,lx1,ly1], outline=border, width=S)
        bb=draw.textbbox((0,0),name,font=fn_name)
        draw.text((lx0+PX2-bb[0], ly0+PY2-bb[1]), name, font=fn_name, fill=TEXT_MAIN)

    # 图例（节点多时显示）
    if USE_RING:
        draw = ImageDraw.Draw(img)
        fn_leg = _font(_FONT_REG, 8*S)
        legends = [
            (RING_COLORS[0], "★ 草人最多"),
            (RING_COLORS[1], "  次多"),
            (EDGE_MUTUAL,   "─ 互草"),
            (EDGE_SINGLE,   "─ 单向草"),
        ]
        lx=14*S; ly=HDR_H+10*S
        for col,txt2 in legends:
            draw.rectangle([lx, ly+2*S, lx+16*S, ly+10*S], fill=col)
            draw.text((lx+20*S, ly), txt2, font=fn_leg, fill=TEXT_MAIN)
            ly += 16*S

    # ── Footer ───────────────────────────────────────────────────
    draw = ImageDraw.Draw(img)
    fy = CH - FOOT_H
    draw.rectangle([0,fy,CW,CH], fill=(255,255,255))
    fn_foot = _font(_FONT_REG, 9*S)
    n_edges = sum(edge_count_full.values())
    foot_txt = f"今日共发生 {n_edges} 次草草 · {n} 人参与"
    _tc(draw, CW//2, fy+(FOOT_H-_th(draw,foot_txt,fn_foot))//2,
        foot_txt, fn_foot, FOOT_FG)

    img.save(out_path, "PNG", optimize=True, compress_level=6)
    return out_path
