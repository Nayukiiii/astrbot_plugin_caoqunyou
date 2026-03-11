"""
我的战绩 —— 单人攻方档案卡 Pillow 渲染
布局：
  Header
  头像 + 昵称 + 称号
  ┌────────────────────┬──────────────────────┐
  │   三格统计（左56%）│  迷你折线图（右44%） │
  └────────────────────┴──────────────────────┘
  打倒最多的目标 Top3
  Footer
"""
from __future__ import annotations
from PIL import Image, ImageDraw, ImageFont
import sys, os, io, asyncio, json
from datetime import datetime, timedelta
from collections import defaultdict
import aiohttp

_FONT_REG  = "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"
_FONT_MED  = "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc"
_FONT_BOLD = "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc"

if sys.platform == "win32":
    _WIN = os.path.join(os.environ.get("WINDIR", "C:\\Windows"), "Fonts")
    for _var, _cands in [
        ("_FONT_REG",  ["msyh.ttc", "simsun.ttc", "Arial.ttf"]),
        ("_FONT_MED",  ["msyhbd.ttc", "msyh.ttc", "Arial.ttf"]),
        ("_FONT_BOLD", ["msyhbd.ttc", "simhei.ttf", "Arial.ttf"]),
    ]:
        for _c in _cands:
            _p = os.path.join(_WIN, _c)
            if os.path.exists(_p):
                globals()[_var] = _p
                break

def _font(path, size):
    try:    return ImageFont.truetype(path, size)
    except: return ImageFont.load_default()

# ── 色彩 ─────────────────────────────────────────────────────────
BG_PAGE      = (255, 245, 247)
BG_CONT      = (255, 255, 255)
BG_ALT       = (255, 250, 252)
BG_CHART     = (253, 248, 250)
SEP          = (255, 240, 245)
ACCENT       = (200,  90, 124)
ACCENT_LIGHT = (255, 183, 197)
ACCENT_SOFT  = (255, 214, 224)
DEEP         = (160,  54,  90)
TEXT_PRIMARY = ( 74,  63,  75)
TEXT_LIGHT   = (255, 255, 255)
TEXT_GRAY    = (138, 127, 139)
RANK1        = (255, 215,   0)
RANK2        = (192, 192, 192)
RANK3        = (205, 127,  50)
CHART_LINE   = (200,  90, 124)
ACCENT2      = (150,  80, 160)
ACCENT2_LIGHT= (210, 170, 230)
ACCENT2_SOFT = (230, 200, 245)
DEEP2        = (100,  40, 130)
# 统计标签专属色（更柔和）
C_CAO   = (220,  80, 115)   # 草人次数：玫红
C_CAO2  = (175,  45,  80)
C_QUN   = (210, 120,  35)   # 草全群：橙
C_QUN2  = (160,  80,  20)
C_FAN   = (135,  75, 155)   # 被反草：紫
C_FAN2  = ( 90,  40, 115)


def _tw(draw, text, fnt):
    bb = draw.textbbox((0, 0), text, font=fnt); return bb[2] - bb[0]

def _th(draw, text, fnt):
    bb = draw.textbbox((0, 0), text, font=fnt); return bb[3] - bb[1]

def _tc(draw, cx, y, text, fnt, fill):
    draw.text((cx - _tw(draw, text, fnt) // 2, y), text, font=fnt, fill=fill)

def _grad_h(draw, x0, y0, x1, y1, cl, cr):
    w = x1 - x0
    if w <= 0: return
    for i in range(w):
        t = i / (w - 1) if w > 1 else 0
        c = tuple(int(cl[k] + (cr[k] - cl[k]) * t) for k in range(3))
        draw.line([(x0+i, y0), (x0+i, y1)], fill=c)

def _shadow(draw, x0, y0, x1, y1, s, col=None):
    draw.rectangle([x0+s, y0+s, x1+s, y1+s], fill=col or ACCENT_SOFT)

def _paste_avatar(img, avatar_bytes, x, y, size):
    try:
        av = Image.open(io.BytesIO(avatar_bytes)).convert("RGB")
        av = av.resize((size, size), Image.LANCZOS)
        img.paste(av, (x, y))
    except Exception:
        pass

async def _fetch_qq_avatar(qq: str, cache_dir: str, size: int = 100) -> bytes | None:
    cache_path = os.path.join(cache_dir, f"avatar_{qq}.jpg")
    if os.path.exists(cache_path):
        try:
            with open(cache_path, "rb") as f:
                return f.read()
        except Exception:
            pass
    url = f"https://q4.qlogo.cn/headimg_dl?dst_uin={qq}&spec={size}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.read()
                    try:
                        os.makedirs(cache_dir, exist_ok=True)
                        with open(cache_path, "wb") as f:
                            f.write(data)
                    except Exception:
                        pass
                    return data
    except Exception:
        pass
    return None


def _load_titles(json_path: str) -> dict:
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"ml_tiers": [], "count_tiers": []}

def _pick_title(tiers: list, key_min: str, value) -> list:
    for tier in tiers:
        if value >= tier[key_min]:
            return [t for t in tier.get("titles", []) if t]
    return []

def _pick_combined_title(cfg: dict, ml_value: float, count_value: int) -> str | None:
    import random
    pool = (
        _pick_title(cfg.get("ml_tiers", []),    "min_ml",    ml_value) +
        _pick_title(cfg.get("count_tiers", []), "min_count", count_value)
    )
    return random.choice(pool) if pool else None


def _draw_stat_pill(draw, cx, cy, text, fnt, c1, c2, S):
    """
    以 (cx, cy) 为中心绘制渐变胶囊标签，文字精确垂直居中。
    返回 (pill_w, pill_h)。
    """
    PX = 14 * S
    PY =  8 * S
    tw = _tw(draw, text, fnt)
    th = _th(draw, text, fnt)
    pw = tw + PX * 2
    ph = th + PY * 2
    x0 = cx - pw // 2
    y0 = cy - ph // 2
    # 投影
    draw.rectangle([x0+2, y0+2, x0+pw+2, y0+ph+2], fill=(*c2, 80) if len(c2)==3 else c2)
    # 渐变填充
    _grad_h(draw, x0, y0, x0+pw, y0+ph, c1, c2)
    # 文字：使用 textbbox 的精确偏移，确保视觉居中
    bb   = draw.textbbox((0, 0), text, font=fnt)
    tx   = x0 + pw // 2 - (bb[2] - bb[0]) // 2 - bb[0]
    ty   = y0 + ph // 2 - (bb[3] - bb[1]) // 2 - bb[1]
    draw.text((tx, ty), text, font=fnt, fill=TEXT_LIGHT)
    return pw, ph


def _draw_mini_trend(img, records, x0, y0, w, h, S):
    """折线图小块，无Y轴数字标签"""
    draw = ImageDraw.Draw(img)
    draw.rectangle([x0, y0, x0+w, y0+h], fill=BG_CHART)
    draw.line([(x0, y0), (x0+w, y0)], fill=ACCENT_SOFT, width=2*S)

    today = datetime.now().date()
    days  = [(today - timedelta(days=29-i)) for i in range(30)]
    day_count = defaultdict(int)
    for r in records:
        try:
            d = datetime.fromtimestamp(r["ts"]).date()
            day_count[d.isoformat()] += 1
        except Exception:
            pass
    values  = [day_count.get(d.isoformat(), 0) for d in days]
    max_val = max(values) if any(v > 0 for v in values) else 1

    PAD_L = 6*S; PAD_R = 6*S; PAD_T = 16*S; PAD_B = 16*S
    cw = w - PAD_L - PAD_R
    ch = h - PAD_T - PAD_B
    n  = 30

    def px(i): return x0 + PAD_L + int(i * cw / (n-1))
    def py(v): return y0 + PAD_T + ch - int((v / max_val) * ch)

    fn_title = _font(_FONT_MED, 8*S)
    draw.text((x0 + PAD_L, y0 + 3*S), "近30天草人趋势", font=fn_title, fill=ACCENT)

    if max_val > 0:
        fn_pk  = _font(_FONT_REG, 7*S)
        pk_txt = f"峰值 {max_val}次"
        draw.text((x0+w-_tw(draw,pk_txt,fn_pk)-4*S, y0+3*S), pk_txt, font=fn_pk, fill=TEXT_GRAY)

    gy = y0 + PAD_T + ch - int(0.5*ch)
    draw.line([(x0+PAD_L, gy), (x0+PAD_L+cw, gy)], fill=ACCENT_SOFT, width=S)

    pts = [(px(i), py(v)) for i, v in enumerate(values)]

    if any(v > 0 for v in values):
        base_y   = y0 + PAD_T + ch
        fill_pts = [(px(0), base_y)] + pts + [(px(n-1), base_y)]
        overlay  = Image.new("RGBA", img.size, (0, 0, 0, 0))
        ov_draw  = ImageDraw.Draw(overlay)
        ov_draw.polygon(fill_pts, fill=(*ACCENT_SOFT, 130))
        merged   = Image.alpha_composite(img.convert("RGBA"), overlay).convert("RGB")
        img.paste(merged, (0, 0))

    draw = ImageDraw.Draw(img)
    for i in range(n-1):
        draw.line([pts[i], pts[i+1]], fill=CHART_LINE, width=2*S)
    for i, v in enumerate(values):
        r    = 3*S if v > 0 else 2*S
        fill = ACCENT if v > 0 else ACCENT_SOFT
        draw.ellipse([pts[i][0]-r, pts[i][1]-r, pts[i][0]+r, pts[i][1]+r], fill=fill)

    fn_date = _font(_FONT_REG, 7*S)
    for i in (0, 14, 29):
        lbl = days[i].strftime("%m/%d")
        lw  = _tw(draw, lbl, fn_date)
        lx  = max(x0+PAD_L, min(px(i)-lw//2, x0+PAD_L+cw-lw))
        draw.text((lx, y0+PAD_T+ch+3*S), lbl, font=fn_date, fill=TEXT_GRAY)


async def render_my_battle(
    user_qq: str,
    user_name: str,
    attacker_records: list[dict],
    fancao_count: int,
    caoquanqun_count: int,
    top_victims: list[dict],
    out_path: str,
    cache_dir: str | None = None,
    titles_path: str | None = None,
    scale: int = 2,
):
    S = scale
    W = 580 * S
    PAD = 16 * S

    HDR_H     = 52 * S
    PROFILE_H = 96 * S
    MID_H     = 110 * S
    LEFT_W    = int(W * 0.56)
    RIGHT_W   = W - LEFT_W
    TOP_HDR_H  = 34 * S
    TOP_ITEM_H = 58 * S
    top_n      = min(len(top_victims), 3)
    TOP_BODY_H = TOP_HDR_H + TOP_ITEM_H * max(top_n, 1)
    FOOT_H     = 32 * S

    H = HDR_H + PROFILE_H + MID_H + TOP_BODY_H + FOOT_H

    _cd      = cache_dir or os.path.join(os.path.dirname(out_path), "avatar_cache")
    all_uids = [user_qq] + [v["uid"] for v in top_victims]
    tasks    = {qq: _fetch_qq_avatar(qq, _cd) for qq in set(all_uids) if qq}
    results  = {}
    if tasks:
        fetched = await asyncio.gather(*tasks.values(), return_exceptions=True)
        for qq, data in zip(tasks.keys(), fetched):
            results[qq] = data if isinstance(data, bytes) else None

    _titles_cfg = _load_titles(titles_path) if titles_path else {"ml_tiers": [], "count_tiers": []}

    img  = Image.new("RGB", (W, H), BG_PAGE)
    draw = ImageDraw.Draw(img)

    # ── Header ──────────────────────────────────────────────────
    _grad_h(draw, 0, 0, W, HDR_H, ACCENT_LIGHT, DEEP)
    fn_hdr = _font(_FONT_BOLD, 20*S)
    _tc(draw, W//2, (HDR_H - _th(draw, "A", fn_hdr))//2, "我的战绩", fn_hdr, TEXT_LIGHT)

    cy = HDR_H

    # ── 头像 + 昵称 ─────────────────────────────────────────────
    draw.rectangle([0, cy, W, cy+PROFILE_H], fill=BG_CONT)
    draw.line([(0, cy+PROFILE_H-2*S), (W, cy+PROFILE_H-2*S)], fill=ACCENT_SOFT, width=2*S)

    AV_SZ = 64 * S
    av_x  = PAD
    av_y  = cy + (PROFILE_H - AV_SZ) // 2
    _shadow(draw, av_x, av_y, av_x+AV_SZ, av_y+AV_SZ, 4*S, ACCENT_SOFT)
    av_data = results.get(user_qq)
    if av_data:
        _paste_avatar(img, av_data, av_x, av_y, AV_SZ)
        draw = ImageDraw.Draw(img)
        draw.rectangle([av_x, av_y, av_x+AV_SZ, av_y+AV_SZ], outline=ACCENT_LIGHT, width=2*S)
    else:
        _grad_h(draw, av_x, av_y, av_x+AV_SZ, av_y+AV_SZ, ACCENT_LIGHT, ACCENT)
        draw.rectangle([av_x, av_y, av_x+AV_SZ, av_y+AV_SZ], outline=ACCENT_LIGHT, width=2*S)
        fn_ini = _font(_FONT_BOLD, 22*S)
        ini    = user_name[0] if user_name else "?"
        _tc(draw, av_x+AV_SZ//2, av_y+(AV_SZ-_th(draw,ini,fn_ini))//2, ini, fn_ini, TEXT_LIGHT)

    total_count = len(attacker_records)
    total_ml    = sum(r.get("ml", 0.0) for r in attacker_records)
    title_tag   = _pick_combined_title(_titles_cfg, total_ml, total_count)

    fn_nm = _font(_FONT_BOLD, 15*S)
    fn_tt = _font(_FONT_REG,   9*S)
    nx    = av_x + AV_SZ + 14*S
    nm_y  = cy + PROFILE_H//2 - _th(draw, user_name, fn_nm) - (5*S if title_tag else 0)
    draw.text((nx, nm_y), user_name, font=fn_nm, fill=TEXT_PRIMARY)

    if title_tag:
        tx, ty  = nx, nm_y + _th(draw, user_name, fn_nm) + 4*S
        TAG_PX, TAG_PY = 6*S, 3*S
        bw = _tw(draw, title_tag, fn_tt) + TAG_PX*2
        bh = _th(draw, title_tag, fn_tt) + TAG_PY*2
        _shadow(draw, tx, ty, tx+bw, ty+bh, 2*S, ACCENT2_SOFT)
        _grad_h(draw, tx, ty, tx+bw, ty+bh, ACCENT2_LIGHT, ACCENT2)
        bb  = draw.textbbox((0,0), title_tag, font=fn_tt)
        ttx = tx + TAG_PX - bb[0]
        tty = ty + TAG_PY - bb[1]
        draw.text((ttx, tty), title_tag, font=fn_tt, fill=TEXT_LIGHT)

    cy += PROFILE_H

    # ── 中间：左统计 | 右折线图 ──────────────────────────────────
    draw.rectangle([0, cy, W, cy+MID_H], fill=BG_ALT)
    draw.line([(0, cy+MID_H-2*S), (W, cy+MID_H-2*S)], fill=SEP, width=2*S)
    draw.line([(LEFT_W, cy+10*S), (LEFT_W, cy+MID_H-10*S)], fill=SEP, width=2*S)

    # 三格统计：标签文字 + 下方胶囊数值，整体垂直居中在左侧区域
    stats = [
        ("草人次数", f"{total_count}次",     C_CAO,  C_CAO2),
        ("草全群",   f"{caoquanqun_count}次", C_QUN,  C_QUN2),
        ("被反草",   f"{fancao_count}次",     C_FAN,  C_FAN2),
    ]
    col_w  = LEFT_W // 3
    fn_lbl = _font(_FONT_REG,   8*S)
    fn_val = _font(_FONT_BOLD, 14*S)

    # 计算整体内容高度（标签+间距+胶囊），用于垂直居中
    lbl_h  = _th(draw, "草", fn_lbl)
    val_h  = _th(draw, "次", fn_val) + 8*S*2   # 胶囊高 = 文字高 + PY*2
    gap    = 6 * S
    blk_h  = lbl_h + gap + val_h
    blk_y0 = cy + (MID_H - blk_h) // 2

    for i, (lbl, val, c1, c2) in enumerate(stats):
        cx_col = i * col_w + col_w // 2
        # 标签文字
        lbl_y = blk_y0
        _tc(draw, cx_col, lbl_y, lbl, fn_lbl, TEXT_GRAY)
        # 胶囊数值：以中心点绘制
        pill_cy = blk_y0 + lbl_h + gap + val_h // 2
        _draw_stat_pill(draw, cx_col, pill_cy, val, fn_val, c1, c2, S)
        # 分隔线
        if i < 2:
            draw.line([(i+1)*col_w, cy+14*S, (i+1)*col_w, cy+MID_H-14*S],
                      fill=SEP, width=2*S)

    # 迷你折线图
    cp = 8 * S
    _draw_mini_trend(img, attacker_records,
                     LEFT_W+cp, cy+cp, RIGHT_W-cp*2, MID_H-cp*2, S)
    draw = ImageDraw.Draw(img)

    cy += MID_H

    # ── Top3 ────────────────────────────────────────────────────
    draw.rectangle([0, cy, W, cy+TOP_BODY_H], fill=BG_CONT)
    draw.rectangle([0, cy, W, cy+TOP_HDR_H], fill=BG_CONT)
    draw.line([(0, cy+TOP_HDR_H-2*S), (W, cy+TOP_HDR_H-2*S)], fill=ACCENT_SOFT, width=2*S)
    fn_tophdr = _font(_FONT_BOLD, 11*S)
    _tc(draw, W//2, cy+(TOP_HDR_H-_th(draw,"A",fn_tophdr))//2,
        "打倒最多的目标 Top 3", fn_tophdr, ACCENT)
    cy += TOP_HDR_H

    if not top_victims:
        fe = _font(_FONT_REG, 11*S)
        _tc(draw, W//2, cy+(TOP_ITEM_H-_th(draw,"A",fe))//2,
            "还没有草过任何人～", fe, TEXT_GRAY)
        cy += TOP_ITEM_H
    else:
        RANK_CL = [RANK1, RANK2, RANK3]
        fn_nm2  = _font(_FONT_MED, 11*S)
        fn_sub2 = _font(_FONT_REG,  9*S)
        fn_cv   = _font(_FONT_REG, 10*S)
        AV2     = 36 * S

        for i, v in enumerate(top_victims[:3]):
            iy = cy + i * TOP_ITEM_H
            draw.rectangle([0, iy, W, iy+TOP_ITEM_H],
                           fill=BG_ALT if i%2==0 else BG_CONT)
            draw.line([(0, iy+TOP_ITEM_H-2*S), (W, iy+TOP_ITEM_H-2*S)],
                      fill=SEP, width=2*S)

            rnum  = str(i+1)
            fn_rk = _font(_FONT_BOLD, 17*S if i==0 else 14*S)
            rw    = _tw(draw, rnum, fn_rk)
            rh    = _th(draw, rnum, fn_rk)
            draw.text((PAD+(20*S-rw)//2, iy+(TOP_ITEM_H-rh)//2),
                      rnum, font=fn_rk, fill=RANK_CL[i])

            avx = PAD + 26*S
            avy = iy + (TOP_ITEM_H-AV2) // 2
            _shadow(draw, avx, avy, avx+AV2, avy+AV2, 3*S, ACCENT_SOFT)
            av_d = results.get(v["uid"])
            if av_d:
                _paste_avatar(img, av_d, avx, avy, AV2)
                draw = ImageDraw.Draw(img)
                draw.rectangle([avx, avy, avx+AV2, avy+AV2], outline=ACCENT_LIGHT, width=2*S)
            else:
                _grad_h(draw, avx, avy, avx+AV2, avy+AV2, ACCENT_LIGHT, ACCENT)
                draw.rectangle([avx, avy, avx+AV2, avy+AV2], outline=ACCENT_LIGHT, width=2*S)
                fn_i = _font(_FONT_BOLD, 12*S)
                ini2 = v["name"][0] if v["name"] else "?"
                _tc(draw, avx+AV2//2, avy+(AV2-_th(draw,ini2,fn_i))//2, ini2, fn_i, TEXT_LIGHT)
            draw = ImageDraw.Draw(img)

            nx2     = avx + AV2 + 10*S
            cnt_txt = f"{v['count']} 次"
            cv_bw   = _tw(draw, cnt_txt, fn_cv) + 14*S
            max_nw  = W - nx2 - cv_bw - PAD - 20*S
            name2   = v["name"]
            while _tw(draw, name2, fn_nm2) > max_nw and len(name2) > 1:
                name2 = name2[:-1]
            if name2 != v["name"]:
                name2 = name2[:-1] + "…"

            # 名字垂直居中
            bb_nm = draw.textbbox((0,0), name2, font=fn_nm2)
            draw.text((nx2, iy+TOP_ITEM_H//2-_th(draw,name2,fn_nm2)-2*S),
                      name2, font=fn_nm2, fill=TEXT_PRIMARY)
            draw.text((nx2, iy+TOP_ITEM_H//2+2*S),
                      f"被草 {v['count']} 次", font=fn_sub2, fill=TEXT_GRAY)

            cv_bh = _th(draw, cnt_txt, fn_cv) + 8*S
            cv_x  = W - PAD - cv_bw
            cv_y  = iy + (TOP_ITEM_H-cv_bh) // 2
            _shadow(draw, cv_x, cv_y, cv_x+cv_bw, cv_y+cv_bh, 2*S)
            _grad_h(draw, cv_x, cv_y, cv_x+cv_bw, cv_y+cv_bh, ACCENT, DEEP)
            bb_cv = draw.textbbox((0,0), cnt_txt, font=fn_cv)
            draw.text((cv_x+7*S - bb_cv[0], cv_y+4*S - bb_cv[1]), cnt_txt, font=fn_cv, fill=TEXT_LIGHT)

        cy += TOP_ITEM_H * max(top_n, 1)

    # ── Footer ───────────────────────────────────────────────────
    cy_foot = H - FOOT_H
    draw.rectangle([0, cy_foot, W, H], fill=BG_CONT)
    fn_foot = _font(_FONT_REG, 10*S)
    _tc(draw, W//2, cy_foot+(FOOT_H-_th(draw,"A",fn_foot))//2,
        "统计范围：近30天 · 数据每日滚动更新", fn_foot, ACCENT)

    img.save(out_path, "PNG", optimize=True, compress_level=6)
    return out_path
