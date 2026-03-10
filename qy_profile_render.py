"""
单人体内档案卡 Pillow 渲染
展示内容：
  - 大头像 + 昵称 + 称号标签
  - 本月体内总量 / 被注入次数 / 群内排名
  - 近30天被注入趋势折线图（按天聚合）
  - 被注入来源 Top3（谁草了你最多）
async 版本，内部自动下载 QQ 头像
"""
from __future__ import annotations
from PIL import Image, ImageDraw, ImageFont
import sys, os, io, asyncio, math
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

# ── 色彩系统（与其他卡片一致）──────────────────────────────
BG_PAGE      = (255, 245, 247)
BG_CONT      = (255, 255, 255)
BG_ALT       = (255, 250, 252)
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
CHART_FILL   = (255, 214, 224, 120)   # 带透明度，用于折线下方填充
ACCENT2      = (150,  80, 160)
ACCENT2_LIGHT= (210, 170, 230)
ACCENT2_SOFT = (230, 200, 245)


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

def _grad_v(draw, x0, y0, x1, y1, ct, cb):
    h = y1 - y0
    if h <= 0: return
    for i in range(h):
        t = i / (h - 1) if h > 1 else 0
        c = tuple(int(ct[k] + (cb[k] - ct[k]) * t) for k in range(3))
        draw.line([(x0, y0+i), (x1, y0+i)], fill=c)

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
        import json
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
    pool = (_pick_title(cfg.get("ml_tiers", []), "min_ml", ml_value) +
            _pick_title(cfg.get("count_tiers", []), "min_count", count_value))
    return random.choice(pool) if pool else None

def _fmt_ml(ml: float) -> str:
    if ml < 1.0:
        return f"{ml * 1000:.0f} µL"
    elif ml < 1000.0:
        return f"{ml:.1f} mL"
    else:
        return f"{ml / 1000:.2f} L"


def _draw_trend_chart(
    img: Image.Image,
    draw: ImageDraw.ImageDraw,
    records: list[dict],  # [{"ts": float, "ml": float}, ...]
    x0: int, y0: int, w: int, h: int,
    S: int,
):
    """
    在 (x0,y0,x0+w,y0+h) 区域内绘制近30天注入量折线图（按天聚合）。
    records 为30天内的被注入记录。
    """
    # 生成近30天的日期列表
    today = datetime.now().date()
    days  = [(today - timedelta(days=29 - i)) for i in range(30)]
    day_ml: dict[str, float] = defaultdict(float)
    for r in records:
        d = datetime.fromtimestamp(r["ts"]).date()
        day_ml[d.isoformat()] += r["ml"]

    values = [day_ml.get(d.isoformat(), 0.0) for d in days]
    max_val = max(values) if any(v > 0 for v in values) else 1.0

    PAD_L = 8 * S
    PAD_R = 8 * S
    PAD_T = 10 * S
    PAD_B = 20 * S  # 留给日期标签

    chart_w = w - PAD_L - PAD_R
    chart_h = h - PAD_T - PAD_B
    n = len(days)

    def _px(i):
        return x0 + PAD_L + int(i * chart_w / (n - 1))

    def _py(v):
        return y0 + PAD_T + chart_h - int((v / max_val) * chart_h)

    # 背景
    draw.rectangle([x0, y0, x0 + w, y0 + h], fill=BG_CONT)
    draw.line([(x0, y0), (x0 + w, y0)], fill=SEP, width=2*S)

    # 网格线（3条）
    fn_axis = _font(_FONT_REG, 9*S)
    for level in (0.25, 0.5, 0.75, 1.0):
        gy = y0 + PAD_T + chart_h - int(level * chart_h)
        draw.line([(x0 + PAD_L, gy), (x0 + PAD_L + chart_w, gy)],
                  fill=ACCENT_SOFT, width=S)
        if level < 1.0:
            label = _fmt_ml(max_val * level)
            draw.text((x0 + PAD_L - _tw(draw, label, fn_axis) - 2*S, gy - _th(draw, "A", fn_axis) // 2),
                      label, font=fn_axis, fill=TEXT_GRAY)

    # 折线填充区（RGBA overlay）
    if any(v > 0 for v in values):
        fill_pts = [(_px(0), y0 + PAD_T + chart_h)]
        for i, v in enumerate(values):
            fill_pts.append((_px(i), _py(v)))
        fill_pts.append((_px(n - 1), y0 + PAD_T + chart_h))

        overlay = Image.new("RGBA", img.size, (0, 0, 0, 0))
        ov_draw = ImageDraw.Draw(overlay)
        ov_draw.polygon(fill_pts, fill=(*ACCENT_SOFT, 140))
        img.paste(Image.alpha_composite(img.convert("RGBA"), overlay).convert("RGB"),
                  (0, 0))
        # 重新拿 draw（paste 后 img 对象变了，需要重建 draw）
        # 但这里 draw 是外部传进来的，用完后调用方会继续用同一个 img
        # 所以用 img.paste 的方式会使外部 draw 对象指向旧数据
        # 换一种方式：直接在原图上画半透明多边形（用 ImageDraw 的 polygon + alpha blend）
        # Pillow 标准方式：先创建临时 RGBA 图层再合并
        draw2 = ImageDraw.Draw(img)
        draw2.polygon(fill_pts, fill=(*ACCENT_SOFT, 100))

    # 折线
    draw2 = ImageDraw.Draw(img)
    pts = [(_px(i), _py(v)) for i, v in enumerate(values)]
    for i in range(len(pts) - 1):
        draw2.line([pts[i], pts[i+1]], fill=CHART_LINE, width=2*S)

    # 数据点
    for i, v in enumerate(values):
        px, py = pts[i]
        r = 4*S if v > 0 else 2*S
        fill = ACCENT if v > 0 else ACCENT_SOFT
        draw2.ellipse([px - r, py - r, px + r, py + r], fill=fill)

    # X 轴日期标签（每5天标一次）
    fn_date = _font(_FONT_REG, 8*S)
    for i, d in enumerate(days):
        if i % 5 == 0 or i == n - 1:
            label = d.strftime("%m/%d")
            lw = _tw(draw2, label, fn_date)
            lx = _px(i) - lw // 2
            lx = max(x0 + PAD_L, min(lx, x0 + PAD_L + chart_w - lw))
            draw2.text((lx, y0 + PAD_T + chart_h + 4*S), label, font=fn_date, fill=TEXT_GRAY)


async def render_qy_profile(
    target_qq: str,
    target_name: str,
    # 月度数据
    month_ml: float,
    month_count: int,
    month_rank: int,        # 在本群的月度排名，0=未上榜
    month_total_users: int, # 本群本月参与人数
    reset_date: str,
    days_left: int,
    hours_left: int,
    # 30天历史记录 [{"ts": float, "ml": float, "attacker_id": str}, ...]
    records_30d: list[dict],
    # 攻击者信息 {attacker_id: {"name": str, "total_ml": float, "count": int}}
    attacker_map: dict,
    out_path: str,
    cache_dir: str | None = None,
    titles_path: str | None = None,
    scale: int = 2,
):
    S = scale
    W = 420 * S

    HDR_H    = 54  * S
    PRO_H    = 150 * S   # 头像 + 名字 + 称号
    STAT_H   = 90  * S   # 三格统计（总量 / 次数 / 排名）
    RES_H    = 44  * S   # 刷新时间
    CHART_H  = 180 * S   # 折线图
    SRC_HDR_H= 38  * S   # "来源" 小标题
    SRC_H    = 90  * S   # Top3攻击者行
    FOOT_H   = 36  * S

    # 若无攻击来源数据则折叠
    top_attackers = sorted(
        attacker_map.values(), key=lambda x: x["total_ml"], reverse=True
    )[:3]
    has_src = bool(top_attackers)
    if not has_src:
        SRC_HDR_H = 0
        SRC_H     = 0

    H = HDR_H + PRO_H + STAT_H + RES_H + CHART_H + SRC_HDR_H + SRC_H + FOOT_H

    # ── 并发下载头像 ──────────────────────────────────
    _cd = cache_dir or os.path.join(os.path.dirname(out_path), "avatar_cache")
    qq_list = [target_qq] + [v["uid"] for v in top_attackers if v.get("uid")]
    tasks   = {qq: _fetch_qq_avatar(qq, _cd) for qq in set(qq_list) if qq}
    results = {}
    if tasks:
        fetched = await asyncio.gather(*tasks.values(), return_exceptions=True)
        for qq, data in zip(tasks.keys(), fetched):
            results[qq] = data if isinstance(data, bytes) else None

    _titles_cfg = _load_titles(titles_path) if titles_path else {"ml_tiers": [], "count_tiers": []}
    title_tag = _pick_combined_title(_titles_cfg, month_ml, month_count)

    img  = Image.new("RGB", (W, H), BG_PAGE)
    draw = ImageDraw.Draw(img)

    # ── Header ──────────────────────────────────────
    _grad_h(draw, 0, 0, W, HDR_H, ACCENT_LIGHT, DEEP)
    fn = _font(_FONT_BOLD, 20*S)
    _tc(draw, W // 2, (HDR_H - _th(draw, "A", fn)) // 2, "体内液体档案", fn, TEXT_LIGHT)

    # ── Profile ──────────────────────────────────────
    py = HDR_H
    _grad_v(draw, 0, py, W, py + PRO_H, (255, 248, 250), BG_CONT)
    draw.line([(0, py + PRO_H - 2*S), (W, py + PRO_H - 2*S)], fill=ACCENT_SOFT, width=2*S)

    AV = 90*S
    ax = W // 2 - AV // 2
    ay = py + 14*S
    _shadow(draw, ax, ay, ax + AV, ay + AV, 6*S)
    av_data = results.get(target_qq)
    if av_data:
        _paste_avatar(img, av_data, ax, ay, AV)
        draw.rectangle([ax, ay, ax + AV, ay + AV], outline=ACCENT, width=3*S)
    else:
        _grad_h(draw, ax, ay, ax + AV, ay + AV, ACCENT_LIGHT, ACCENT)
        draw.rectangle([ax, ay, ax + AV, ay + AV], outline=ACCENT, width=3*S)
        fn_i = _font(_FONT_BOLD, 36*S)
        ini  = target_name[0] if target_name else "?"
        _tc(draw, ax + AV // 2, ay + AV // 2 - _th(draw, ini, fn_i) // 2, ini, fn_i, TEXT_LIGHT)

    fn_nm = _font(_FONT_BOLD, 16*S)
    name_y = ay + AV + 8*S
    _tc(draw, W // 2, name_y, target_name, fn_nm, ACCENT)

    if title_tag:
        ft_t = _font(_FONT_REG, 10*S)
        TAG_PX, TAG_PY = 8*S, 4*S
        bw = _tw(draw, title_tag, ft_t) + TAG_PX * 2
        bh = _th(draw, title_tag, ft_t) + TAG_PY * 2
        tx = W // 2 - bw // 2
        ty = name_y + _th(draw, target_name, fn_nm) + 6*S
        _shadow(draw, tx, ty, tx + bw, ty + bh, 2*S, (214, 190, 230))
        _grad_h(draw, tx, ty, tx + bw, ty + bh, ACCENT2_LIGHT, ACCENT2)
        draw.text((tx + TAG_PX, ty + TAG_PY), title_tag, font=ft_t, fill=TEXT_LIGHT)

    # ── 三格统计 ─────────────────────────────────────
    sy = py + PRO_H
    draw.rectangle([0, sy, W, sy + STAT_H], fill=(255, 250, 252))
    draw.line([(0, sy + STAT_H - 2*S), (W, sy + STAT_H - 2*S)], fill=SEP, width=2*S)

    CM   = 15*S
    CW   = (W - CM * 4) // 3
    CH   = 66*S
    boxy = sy + (STAT_H - CH) // 2

    def _sbox(x, y, w, h, val, lbl, accent_color=ACCENT):
        _shadow(draw, x, y, x + w, y + h, 4*S)
        draw.rectangle([x, y, x + w, y + h], fill=BG_CONT)
        draw.rectangle([x, y, x + w, y + h], outline=ACCENT_LIGHT, width=2*S)
        fv = _font(_FONT_BOLD, 20*S)
        fl = _font(_FONT_REG, 10*S)
        vh = _th(draw, val, fv)
        lh = _th(draw, lbl, fl)
        vy = y + (h - vh - 6*S - lh) // 2
        _tc(draw, x + w // 2, vy,         val, fv, accent_color)
        _tc(draw, x + w // 2, vy + vh + 6*S, lbl, fl, TEXT_GRAY)

    rank_str = f"#{month_rank}" if month_rank > 0 else "未上榜"
    rank_col = RANK1 if month_rank == 1 else (RANK2 if month_rank == 2 else (RANK3 if month_rank == 3 else ACCENT))

    _sbox(CM,            boxy, CW, CH, _fmt_ml(month_ml),    "本月体内总量")
    _sbox(CM * 2 + CW,   boxy, CW, CH, str(month_count),     "本月被注入次数")
    _sbox(CM * 3 + CW*2, boxy, CW, CH, rank_str,             "本月群内排名", rank_col)

    # ── 刷新时间 ─────────────────────────────────────
    ry = sy + STAT_H
    draw.rectangle([0, ry, W, ry + RES_H], fill=BG_CONT)
    draw.line([(0, ry + RES_H - 2*S), (W, ry + RES_H - 2*S)], fill=SEP, width=2*S)
    fr  = _font(_FONT_REG,  11*S)
    frb = _font(_FONT_BOLD, 11*S)
    lh1 = _th(draw, "A", fr)
    tv  = ry + (RES_H - lh1 * 2 - 6*S) // 2
    _tc(draw, W // 2, tv,           f"本月刷新时间：{reset_date}", fr,  TEXT_GRAY)
    _tc(draw, W // 2, tv + lh1 + 6*S, f"距下次刷新：{days_left}天{hours_left}小时", frb, ACCENT)

    # ── 折线图 ───────────────────────────────────────
    cy = ry + RES_H
    # 小标题
    CHART_HDR = 36*S
    draw.rectangle([0, cy, W, cy + CHART_HDR], fill=BG_CONT)
    draw.line([(0, cy + CHART_HDR - 2*S), (W, cy + CHART_HDR - 2*S)], fill=SEP, width=2*S)
    fn_sec = _font(_FONT_BOLD, 13*S)
    draw.text((15*S, cy + (CHART_HDR - _th(draw, "A", fn_sec)) // 2),
              "近30天被注入趋势", font=fn_sec, fill=ACCENT)
    cy += CHART_HDR

    chart_area_h = CHART_H - CHART_HDR
    _draw_trend_chart(img, draw, records_30d, 0, cy, W, chart_area_h, S)

    # ── 注入来源 Top3 ─────────────────────────────────
    if has_src:
        src_y = cy + chart_area_h
        draw.rectangle([0, src_y, W, src_y + SRC_HDR_H], fill=BG_CONT)
        draw.line([(0, src_y + SRC_HDR_H - 2*S), (W, src_y + SRC_HDR_H - 2*S)], fill=SEP, width=2*S)
        fn_sec = _font(_FONT_BOLD, 13*S)
        draw.text((15*S, src_y + (SRC_HDR_H - _th(draw, "A", fn_sec)) // 2),
                  "注入来源 Top3", font=fn_sec, fill=ACCENT)
        src_y += SRC_HDR_H

        draw.rectangle([0, src_y, W, src_y + SRC_H], fill=(255, 250, 252))
        draw.line([(0, src_y + SRC_H - 2*S), (W, src_y + SRC_H - 2*S)], fill=SEP, width=2*S)

        slot_w = W // max(len(top_attackers), 1)
        RANK_COLORS = [RANK1, RANK2, RANK3]

        for idx, attacker in enumerate(top_attackers):
            uid   = attacker.get("uid", "")
            name  = attacker.get("name", "未知")
            total = attacker.get("total_ml", 0.0)
            cnt   = attacker.get("count", 0)
            slot_x = idx * slot_w

            AV3 = 46*S
            avx = slot_x + (slot_w - AV3) // 2
            avy = src_y + 8*S
            _shadow(draw, avx, avy, avx + AV3, avy + AV3, 3*S)
            av_data = results.get(uid)
            if av_data:
                _paste_avatar(img, av_data, avx, avy, AV3)
                draw.rectangle([avx, avy, avx + AV3, avy + AV3],
                               outline=RANK_COLORS[idx] if idx < 3 else ACCENT_LIGHT,
                               width=3*S)
            else:
                _grad_h(draw, avx, avy, avx + AV3, avy + AV3, ACCENT_LIGHT, ACCENT)
                draw.rectangle([avx, avy, avx + AV3, avy + AV3],
                               outline=RANK_COLORS[idx] if idx < 3 else ACCENT_LIGHT,
                               width=3*S)
                fn_i = _font(_FONT_BOLD, 15*S)
                ini  = name[0] if name else "?"
                _tc(draw, avx + AV3 // 2, avy + (AV3 - _th(draw, ini, fn_i)) // 2,
                    ini, fn_i, TEXT_LIGHT)

            fn_sn = _font(_FONT_MED, 10*S)
            fn_sv = _font(_FONT_REG,  9*S)
            # 截断名字
            sname = name
            while _tw(draw, sname, fn_sn) > slot_w - 8*S and len(sname) > 1:
                sname = sname[:-1]
            if sname != name:
                sname = sname[:-1] + "…"
            text_y = avy + AV3 + 4*S
            _tc(draw, slot_x + slot_w // 2, text_y,                    sname,             fn_sn, TEXT_PRIMARY)
            _tc(draw, slot_x + slot_w // 2, text_y + _th(draw,"A",fn_sn) + 3*S,
                _fmt_ml(total), fn_sv, ACCENT)

    # ── Footer ──────────────────────────────────────
    fy = H - FOOT_H
    draw.rectangle([0, fy, W, H], fill=BG_CONT)
    fn = _font(_FONT_REG, 11*S)
    _tc(draw, W // 2, fy + (FOOT_H - _th(draw, "A", fn)) // 2,
        "体内数据每月1日0点自动刷新 · 趋势图统计近30天", fn, ACCENT)

    img.save(out_path, "PNG", optimize=True, compress_level=6)
    return out_path
