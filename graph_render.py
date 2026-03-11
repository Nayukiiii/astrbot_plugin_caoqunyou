"""
草群友关系图 —— Pillow 渲染 v3

布局：networkx Kamada-Kawai（孤立节点额外处理）
边：全量渲染，透明度按频次叠加，密集处形成热力效果
节点：全量显示，按出度缩放大小（草得多的节点更大）
"""
from __future__ import annotations
from PIL import Image, ImageDraw, ImageFont
import os, io, math, asyncio, random, aiohttp, networkx as nx
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
BG_TOP      = (255, 242, 247)
BG_BTM      = (255, 218, 232)
HDR_L       = (255, 183, 197)
HDR_R       = (190,  75, 115)
NODE_FILL   = (255, 245, 248)
NODE_SHADOW = (235, 190, 208)
TEXT_MAIN   = ( 50,  38,  52)
TEXT_LIGHT  = (255, 255, 255)
TEXT_GRAY   = (150, 130, 145)
LABEL_BG    = (255, 255, 255)
FOOT_FG     = (190,  75, 115)
# 节点边框按出度分级（从高到低）
TIER_COLORS = [
    (200,  60, 100),   # top tier：深玫红
    (170,  80, 150),   # 2nd：紫
    ( 80, 140, 200),   # 3rd：蓝
    (150, 150, 155),   # 其余：灰
]
EDGE_MUTUAL = (140,  60, 180)   # 互草：紫
EDGE_ONE    = (200,  80, 120)   # 单向：玫红


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


# ── Kamada-Kawai 布局 ─────────────────────────────────────────────
def _kk_layout(nodes:list[str], edges:list[tuple[str,str]],
               W:int, H:int, PAD:int) -> dict[str,tuple[float,float]]:
    n = len(nodes)
    if n == 0: return {}
    if n == 1: return {nodes[0]: (W/2, H/2)}

    G = nx.DiGraph()
    G.add_nodes_from(nodes)
    G.add_edges_from(edges)
    UG = G.to_undirected()

    # 孤立节点（无任何连边）单独处理，放到外围
    isolated  = [nd for nd in nodes if UG.degree(nd) == 0]
    connected = [nd for nd in nodes if UG.degree(nd) > 0]

    pos: dict[str,tuple[float,float]] = {}

    if connected:
        sub = UG.subgraph(connected)
        # 对不连通图，分组件分别布局
        components = list(nx.connected_components(sub))
        if len(components) == 1:
            raw = nx.kamada_kawai_layout(sub, scale=1.0)
        else:
            # 多连通分量：每个分量单独KK，然后拼在一起
            raw = {}
            cols = math.ceil(math.sqrt(len(components)))
            for ci, comp in enumerate(components):
                sg = sub.subgraph(comp)
                r  = nx.kamada_kawai_layout(sg, scale=0.4) if len(comp)>1 \
                     else {list(comp)[0]: (0.0,0.0)}
                ox = (ci % cols) * 2.2 - cols
                oy = (ci // cols) * 2.2 - cols
                for nd,(x,y) in r.items():
                    raw[nd] = (x+ox, y+oy)

        # 归一化到画布
        xs = [v[0] for v in raw.values()]
        ys = [v[1] for v in raw.values()]
        xmin,xmax = min(xs),max(xs)
        ymin,ymax = min(ys),max(ys)
        xr = xmax-xmin if xmax>xmin else 1
        yr = ymax-ymin if ymax>ymin else 1
        # 留出孤立节点的空间（底部一行）
        iso_strip = (80 if isolated else 0)
        for nd,(x,y) in raw.items():
            px = PAD + (x-xmin)/xr * (W-PAD*2)
            py = PAD + (y-ymin)/yr * (H-PAD*2-iso_strip)
            pos[nd] = (px, py)

    # 孤立节点排在底部一行
    if isolated:
        cols = max(1, len(isolated))
        for i, nd in enumerate(isolated):
            px = PAD + (i+0.5) * (W-PAD*2) / cols
            py = H - PAD - 30
            pos[nd] = (px, py)

    return pos


# ── 圆形头像 ─────────────────────────────────────────────────────
def _paste_circle(img, av_bytes, cx, cy, r, border, bw=3):
    draw = ImageDraw.Draw(img)
    # 投影
    draw.ellipse([cx-r+4, cy-r+4, cx+r+4, cy+r+4], fill=NODE_SHADOW)
    # 底色
    draw.ellipse([cx-r, cy-r, cx+r, cy+r], fill=NODE_FILL, outline=border, width=bw)
    if av_bytes:
        try:
            av = Image.open(io.BytesIO(av_bytes)).convert("RGBA")
            av = av.resize((r*2, r*2), Image.LANCZOS)
            mask = Image.new("L", (r*2,r*2), 0)
            ImageDraw.Draw(mask).ellipse([0,0,r*2-1,r*2-1], fill=255)
            img.paste(av, (cx-r, cy-r), mask)
        except: pass
    ImageDraw.Draw(img).ellipse([cx-r, cy-r, cx+r, cy+r], outline=border, width=bw)


# ── 箭头线（RGBA overlay） ────────────────────────────────────────
def _draw_arrow_rgba(overlay:Image.Image, x1,y1,x2,y2,
                     color_rgb, lw, node_r, offset, alpha):
    draw = ImageDraw.Draw(overlay)
    dx=x2-x1; dy=y2-y1
    dist=math.sqrt(dx*dx+dy*dy)
    if dist < 1: return
    ux=dx/dist; uy=dy/dist; nx=-uy; ny=ux
    AH = max(10, lw*4)
    SP = node_r+3; EP = node_r+AH+2
    sx=x1+ux*SP+nx*offset; sy=y1+uy*SP+ny*offset
    ex=x2-ux*EP+nx*offset; ey=y2-uy*EP+ny*offset
    c = (*color_rgb, alpha)
    draw.line([(sx,sy),(ex,ey)], fill=c, width=lw)
    # 箭头三角
    tip_x=x2-ux*(node_r+2)+nx*offset; tip_y=y2-uy*(node_r+2)+ny*offset
    AW = max(6, lw*3)
    lx=tip_x-ux*AH+nx*AW; ly=tip_y-uy*AH+ny*AW
    rx=tip_x-ux*AH-nx*AW; ry=tip_y-uy*AH-ny*AW
    draw.polygon([(tip_x,tip_y),(lx,ly),(rx,ry)], fill=c)


# ════════════════════════════════════════════════════════════════
# 主渲染
# ════════════════════════════════════════════════════════════════
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
    edge_count: dict[tuple[str,str],int] = {}
    out_deg: dict[str,int] = defaultdict(int)
    in_deg:  dict[str,int] = defaultdict(int)
    for r in records:
        aid=r["attacker_id"]; tid=r["target_id"]
        node_names[aid] = user_map.get(aid) or r.get("attacker_name") or f"用户{aid}"
        node_names[tid] = user_map.get(tid) or r.get("target_name")  or f"用户{tid}"
        key=(aid,tid)
        edge_count[key] = edge_count.get(key,0)+1
        out_deg[aid]   += 1
        in_deg[tid]    += 1

    nodes     = list(node_names.keys())
    n         = len(nodes)
    edges_all = list(edge_count.keys())
    mutual    = {(a,b) for (a,b) in edges_all if (b,a) in edge_count}
    max_cnt   = max(edge_count.values()) if edge_count else 1
    max_out   = max(out_deg.values()) if out_deg else 1

    # 节点出度分位，用于着色和大小
    sorted_by_out = sorted(nodes, key=lambda nd: out_deg.get(nd,0), reverse=True)
    tier_map: dict[str,int] = {}
    for i,nd in enumerate(sorted_by_out):
        if i < max(1, n//10):   tier_map[nd] = 0
        elif i < max(2, n//4):  tier_map[nd] = 1
        elif i < max(3, n//2):  tier_map[nd] = 2
        else:                   tier_map[nd] = 3

    # ── 画布尺寸 ─────────────────────────────────────────────────
    HDR_H  = 64*S
    FOOT_H = 40*S
    # 节点数越多画布越大，保证不重叠
    base   = max(1100, 700 + n*22)
    CW     = base * S
    CH     = (base + 100) * S
    GRAPH_H = CH - HDR_H - FOOT_H
    PAD    = max(80*S, int(CW*0.08))

    # 节点半径：按出度加权，最大/最小有上下限
    def _node_r(nd):
        ratio = out_deg.get(nd,0) / max_out if max_out>0 else 0
        base_r = max(22*S, min(36*S, int(220*S/max(n,1))))
        bonus  = int(ratio * 14 * S)
        return base_r + bonus

    # ── 下载头像 ─────────────────────────────────────────────────
    _cd = cache_dir or os.path.join(os.path.dirname(out_path), "avatar_cache")
    tasks = {qq: _fetch_avatar(qq,_cd) for qq in nodes}
    avatars: dict[str,bytes|None] = {}
    if tasks:
        fetched = await asyncio.gather(*tasks.values(), return_exceptions=True)
        for qq,data in zip(tasks.keys(), fetched):
            avatars[qq] = data if isinstance(data,bytes) else None

    # ── KK布局 ───────────────────────────────────────────────────
    raw_pos = _kk_layout(nodes, edges_all, CW, GRAPH_H, PAD)
    pos = {nd:(x, y+HDR_H) for nd,(x,y) in raw_pos.items()}

    # ── 画布 ─────────────────────────────────────────────────────
    img  = Image.new("RGBA", (CW,CH), (*BG_TOP, 255))
    draw = ImageDraw.Draw(img)
    _grad_v(draw, 0,0,CW,CH, BG_TOP,BG_BTM)

    # ── Header ───────────────────────────────────────────────────
    _grad_h(draw, 0,0,CW,HDR_H, HDR_L,HDR_R)
    fn_hdr = _font(_FONT_BOLD, 22*S)
    txt = f"{group_name} 今日草群友图谱"
    _tc(draw, CW//2, (HDR_H-_th(draw,txt,fn_hdr))//2, txt, fn_hdr, TEXT_LIGHT)

    # ── 边（RGBA overlay，全量渲染）──────────────────────────────
    edge_overlay = Image.new("RGBA", (CW,CH), (0,0,0,0))

    for (aid,tid),cnt in edge_count.items():
        if aid not in pos or tid not in pos: continue
        ax,ay = pos[aid]; tx,ty = pos[tid]
        is_m  = (aid,tid) in mutual
        color = EDGE_MUTUAL if is_m else EDGE_ONE
        # 透明度：全部实心，频次高的更深
        alpha = int(180 + 75 * min(cnt/max(max_cnt,3), 1.0))
        # 线宽：最细2px，频次高加粗
        lw    = max(S*2, min(S*5, S*2 + int(cnt/max_cnt * S*3)))
        # 互草偏移防重叠
        off   = 10*S if is_m else 0.
        nr    = _node_r(tid)
        _draw_arrow_rgba(edge_overlay, ax,ay,tx,ty, color, lw, nr, off, alpha)

    # 合并边层
    img = Image.alpha_composite(img, edge_overlay).convert("RGBA")

    # ── 节点 ─────────────────────────────────────────────────────
    fn_name_big  = _font(_FONT_BOLD, 9*S)
    fn_name_mid  = _font(_FONT_BOLD, 8*S)
    fn_name_sm   = _font(_FONT_REG,  7*S)

    for nd in nodes:
        if nd not in pos: continue
        cx2,cy2 = int(pos[nd][0]), int(pos[nd][1])
        nr      = _node_r(nd)
        tier    = tier_map.get(nd, 3)
        border  = TIER_COLORS[tier]
        bw      = max(2, 3*S//2 + (S if tier==0 else 0))
        _paste_circle(img, avatars.get(nd), cx2,cy2, nr, border, bw)

        draw = ImageDraw.Draw(img)

        # 名字标签
        name   = node_names[nd]
        fn_use = fn_name_big if tier==0 else (fn_name_mid if tier<=1 else fn_name_sm)
        max_nw = nr*2 + 12*S
        while _tw(draw,name,fn_use) > max_nw and len(name)>1: name=name[:-1]
        if name != node_names[nd]: name=name[:-1]+"…"
        nw=_tw(draw,name,fn_use); nh=_th(draw,name,fn_use)
        PX,PY=4*S,2*S
        lx0=cx2-nw//2-PX; ly0=cy2+nr+3*S
        lx1=lx0+nw+PX*2;  ly1=ly0+nh+PY*2
        draw.rectangle([lx0,ly0,lx1,ly1], fill=LABEL_BG)
        draw.rectangle([lx0,ly0,lx1,ly1], outline=border, width=S)
        bb=draw.textbbox((0,0),name,font=fn_use)
        draw.text((lx0+PX-bb[0], ly0+PY-bb[1]), name, font=fn_use, fill=TEXT_MAIN)

    # ── Footer ───────────────────────────────────────────────────
    img_rgb = img.convert("RGB")
    draw    = ImageDraw.Draw(img_rgb)
    fy = CH - FOOT_H
    draw.rectangle([0,fy,CW,CH], fill=(255,255,255))
    fn_foot = _font(_FONT_REG, 9*S)
    total   = sum(edge_count.values())
    txt_f   = f"今日共发生 {total} 次草草 · {n} 人参与"
    _tc(draw, CW//2, fy+(FOOT_H-_th(draw,txt_f,fn_foot))//2, txt_f, fn_foot, FOOT_FG)

    img_rgb.save(out_path, "PNG", optimize=True, compress_level=6)
    return out_path