import asyncio
import json
import math
import os
import random
import secrets
import time
from datetime import datetime

# ============================================================
# 草群友 - 指令名称（在这里修改）
# ============================================================
CMD_CAOQUNYOU         = "草群友"       # 触发草群友（不@随机，@指定）
CMD_CAOQUNYOU_RANKING = "草群友排行"   # 查看谁草得最多
CMD_CAOQUNYOU_GRAPH   = "草群友关系图" # 今日草群友关系图
CMD_QY_BATTLE         = "群友战绩"     # 群友互草战绩排行
CMD_OUTSIDE_RANK      = "杂鱼排行"     # 选外面的杂鱼排行
CMD_QY_BODY           = "群友体内"     # 群友体内液体查询
CMD_RESET_CAOQUNYOU   = "重置草群友"   # 重置今日草群友次数
CMD_QY_PROFILE        = "我的体内"     # 查看单人体内档案（不@=自己，@=指定人）
CMD_CAO_XIAN_DING     = "草限定"       # 草今日限定群友（每逃走一次概率+5%）
CMD_XIAN_DING_INFO    = "限定"         # 查看今日限定是谁
CMD_DEBUG             = "草群友调试"    # 临时调试：显示管理员匹配信息
CMD_CAO_QUAN_QUN      = "草全群"        # 对全群发起草（每天1次独立冷却，静默汇总结果）
CMD_MY_BATTLE         = "我的战绩"      # 查看自己的攻方战绩档案
# ============================================================

import json as _json
import random as _random_mod

def _secrets_roll() -> float:
    """返回 [0, 1) 的密码学真随机浮点数"""
    return secrets.randbelow(100000) / 100000.0


def _calc_fancao_prob(
    fake_pct: int,
    times_today: int,
    user_30d_count: int,
    fancao_base: float,
) -> float:
    """
    计算反草触发概率。
    - Q：逃脱质量（sigmoid），越低越濒死
    - G：累积仇恨，被Q部分压制但轻松逃时保留残余
    - T：今日压力，同上
    - E：真随机熵扰动 ±0.06
    - cap：上限随base线性开放（base=50→0.92，base=100→0.98）
    """
    Q = 1.0 / (1.0 + math.exp(-(fake_pct - 50) / 12.0))
    G = (1.0 - math.exp(-user_30d_count / 15.0)) * ((1.0 - Q) ** 2 + 0.04)
    T = (1.0 - math.exp(-times_today / 4.0)) * ((1.0 - Q) ** 1.5 + 0.02)
    E = (secrets.randbelow(10000) / 10000.0 - 0.5) * 0.12
    base_rate = fancao_base / 100.0
    cap = 0.88 + (fancao_base / 100.0) * 0.10
    raw = base_rate * ((1.0 - Q) ** 2 + G * 0.5 + T * 0.3) + E
    return max(0.01, min(cap, raw))


def _roll_injection_ml(fake_pct: int | None, grudge: float) -> float:
    """
    对数正态双峰注入量。
    - fake_pct=None 表示草群友（非反草），用中性参数
    - 反草时 fake_pct 越低（越濒死）均值越高；grudge 越高方差越大
    - 上限 100L
    """
    if fake_pct is None:
        mu    = 5.3
        sigma = 1.0
    else:
        Q     = 1.0 / (1.0 + math.exp(-(fake_pct - 50) / 12.0))
        berserk_prob = (1.0 - Q) * grudge
        mode_roll    = secrets.randbelow(10000) / 10000.0
        if mode_roll < berserk_prob:
            mu    = 5.3 + (1.0 - Q) * 1.5
            sigma = 1.0 + grudge * 0.5
        else:
            mu    = 4.8 + grudge * 0.5
            sigma = 0.6 + Q * 0.3
    raw = random.lognormvariate(mu=mu, sigma=sigma)
    return round(max(0.5, min(raw, 100000.0)), 1)


def _ml_grade(ml: float) -> str:
    if ml >= 4000:
        return "雕王"
    elif ml >= 2000:
        return "半雕王"
    elif ml >= 800:
        return "半死不活"
    elif ml >= 400:
        return "杂鱼杂鱼就只有这点吗真是杂～鱼～"
    elif ml >= 200:
        return "杂鱼杂鱼就只有这点吗真是杂～鱼～"
    else:
        return "杂鱼杂鱼就只有这点吗真是杂～鱼～"


def _load_comments(json_path: str) -> list:
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            return _json.load(f).get("ml_tiers", [])
    except Exception:
        return []

def _pick_comment(tiers: list, ml: float) -> str | None:
    for tier in tiers:
        if ml >= tier["min_ml"]:
            comments = [c for c in tier.get("comments", []) if c]
            if comments:
                return _random_mod.choice(comments)
            return None
    return None

def _load_fancao_comments(json_path: str) -> list:
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            return _json.load(f).get("grudge_tiers", [])
    except Exception:
        return []

def _pick_fancao_comment(tiers: list, grudge: float) -> str | None:
    for tier in tiers:
        if grudge >= tier["min_grudge"]:
            comments = [c for c in tier.get("comments", []) if c]
            if comments:
                return _random_mod.choice(comments)
            return None
    return None

import astrbot.api.message_components as Comp
from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star
from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import (
    AiocqhttpMessageEvent,
)
from astrbot.core.utils.astrbot_path import get_astrbot_plugin_data_path

from .onebot_api import extract_message_id


def load_json(path: str, default):
    """读取 JSON 文件，失败返回 default"""
    import json as _j
    try:
        with open(path, "r", encoding="utf-8") as f:
            return _j.load(f)
    except Exception:
        return default

def save_json(path: str, data) -> None:
    """写入 JSON 文件"""
    import json as _j
    try:
        with open(path, "w", encoding="utf-8") as f:
            _j.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        pass

def is_allowed_group(group_id: str, config) -> bool:
    """检查群白/黑名单"""
    whitelist = config.get("whitelist_groups", [])
    blacklist = config.get("blacklist_groups", [])
    if blacklist and str(group_id) in [str(g) for g in blacklist]:
        return False
    if whitelist and str(group_id) not in [str(g) for g in whitelist]:
        return False
    return True
from .qy_body_render import render_qy_body as _render_qy_body
from .qy_battle_render import render_qy_battle as _render_qy_battle
from .outside_rank_render import render_outside_rank as _render_outside_rank
from .qy_profile_render import render_qy_profile as _render_qy_profile
from .my_battle_render import render_my_battle as _render_my_battle


def _fmt_ml(ml: float) -> str:
    if ml < 1.0:
        return f"{ml * 1000:.0f} µL"
    elif ml < 1000.0:
        return f"{ml:.1f} mL"
    else:
        return f"{ml / 1000:.2f} L"


class CaoQunYouPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig = None):
        super().__init__(context)
        self.config = config

        self.curr_dir = os.path.dirname(__file__)

        self._withdraw_tasks: set[asyncio.Task] = set()

        self.data_dir = os.path.join(get_astrbot_plugin_data_path(), "caoqunyou_plugin")
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir, exist_ok=True)

        # 数据文件
        self.cao_stats_file    = os.path.join(self.data_dir, "cao_stats.json")
        self.cao_records_file  = os.path.join(self.data_dir, "cao_records.json")
        self.cao_daily_file    = os.path.join(self.data_dir, "cao_daily.json")
        self.qy_body_file      = os.path.join(self.data_dir, "qy_body.json")
        self.qy_battle_file    = os.path.join(self.data_dir, "qy_battle.json")
        self.outside_stats_file= os.path.join(self.data_dir, "outside_stats.json")

        self.cao_stats    = load_json(self.cao_stats_file,    {})
        self.cao_records  = load_json(self.cao_records_file,  {"date": "", "groups": {}})
        self.cao_daily    = load_json(self.cao_daily_file,    {"date": "", "groups": {}})
        self.qy_body_data = load_json(self.qy_body_file,      {})
        self.qy_battle_data   = load_json(self.qy_battle_file,   {})
        self.outside_stats_data = load_json(self.outside_stats_file, {})
        self.xian_ding_file     = os.path.join(self.data_dir, "xian_ding.json")
        self.xian_ding_data     = load_json(self.xian_ding_file, {})
        # xian_ding_data 结构: {group_id: {"date": "YYYY-MM-DD", "uid": str, "name": str}}

        # 草全群冷却 & 历史记录
        # cao_quan_qun_cd:  {group_id: {user_id: "YYYY-MM-DD"}}  —— 每人每天1次
        # cao_quan_qun_log: {group_id: {user_id: [{"ts":float,"success":int,"escaped":int,"fancaoed":int}, ...]}}
        self.cao_quan_qun_cd_file  = os.path.join(self.data_dir, "cao_quan_qun_cd.json")
        self.cao_quan_qun_log_file = os.path.join(self.data_dir, "cao_quan_qun_log.json")
        self.cao_quan_qun_cd       = load_json(self.cao_quan_qun_cd_file,  {})
        self.cao_quan_qun_log      = load_json(self.cao_quan_qun_log_file, {})

        # 限定草 逃走次数累计（当天内存，重启清零） {group_id: {user_id: escape_count}}
        self._xd_escapes: dict[str, dict[str, int]] = {}

        # 等待内外选择 {group_id: {user_id: True}}
        self._cao_pending: dict[str, dict[str, bool]] = {}

        # 反草等待状态 {group_id: {user_id: True}}
        self._fancao_pending: dict[str, dict[str, bool]] = {}
        # 反草meta {group_id: {user_id: {fake_pct, grudge, attacker_id, attacker_name, target_id, target_name}}}
        self._fancao_meta: dict[str, dict[str, dict]] = {}

        # 重置尝试次数 {date: {group_id: {user_id: int}}}
        self._reset_attempts: dict[str, dict[str, dict[str, int]]] = {}

        self._body_comments    = _load_comments(os.path.join(self.curr_dir, "qy_body_comments.json"))
        self._battle_comments  = _load_comments(os.path.join(self.curr_dir, "qy_battle_comments.json"))
        self._fancao_comments  = _load_fancao_comments(os.path.join(self.curr_dir, "qy_fancao_comments.json"))

        logger.info(f"草群友插件已加载。数据目录: {self.data_dir}")

        # 启动每日12点推送任务
        self._daily_announce_task: asyncio.Task | None = asyncio.create_task(
            self._daily_announce_loop()
        )

    # ============================================================
    # 每日12点定时推送
    # ============================================================

    # ----------------------------------------------------------------
    # ★ 每日推送文案 —— 在这里自定义
    # {name} = 限定群友昵称
    # ----------------------------------------------------------------
    DAILY_ANNOUNCE_TEXT = (
        "今日限定群友是【{name}】！\n"
        # 在这里继续追加你的文案，例如：
        # "快来草他/她！\n"
        # "今日限定概率加成中～"
    )

    async def _daily_announce_loop(self):
        """等到下一个12:00，然后每24小时执行一次推送。"""
        import math as _math
        try:
            while True:
                now = datetime.now()
                ah = int(self.config.get("announce_hour", 12))
                am = int(self.config.get("announce_minute", 0))
                target = now.replace(hour=ah, minute=am, second=0, microsecond=0)
                if target <= now:
                    from datetime import timedelta
                    target = target + timedelta(days=1)
                wait_sec = (target - now).total_seconds()
                logger.info(f"[草限定] 下次推送在 {target}，等待 {wait_sec:.0f}s")
                await asyncio.sleep(wait_sec)
                await self._do_daily_announce()
                # 推完等24小时再循环（避免秒级误差导致重复触发）
                await asyncio.sleep(60)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"[草限定] 定时推送循环异常: {e}")

    async def _do_daily_announce(self):
        """向所有允许的群推送今日限定，抽取新限定并发公告+头像。"""
        # 收集需要推送的群列表
        announce_groups = self.config.get("announce_groups", [])
        whitelist       = self.config.get("whitelist_groups", [])
        blacklist       = self.config.get("blacklist_groups", [])

        if announce_groups:
            # 优先用专用推送列表，同时过滤黑名单
            target_groups = [str(g) for g in announce_groups
                             if str(g) not in [str(b) for b in blacklist]]
        elif whitelist:
            # 没有推送列表时退回功能白名单
            target_groups = [str(g) for g in whitelist]
        else:
            # 兜底：推送所有曾经活跃过的群
            known = set()
            for d in (self.cao_stats, self.cao_records.get("groups", {}),
                      self.qy_body_data, self.xian_ding_data):
                known.update(d.keys())
            target_groups = [g for g in known if str(g) not in [str(b) for b in blacklist]]

        for group_id in target_groups:
            try:
                await self._announce_one_group(group_id)
            except Exception as e:
                logger.warning(f"[草限定] 群 {group_id} 推送失败: {e}")

    async def _announce_one_group(self, group_id: str):
        """为单个群抽取新限定并发推送消息。"""
        # 拉群成员
        members = []
        member_map = {}
        try:
            # 需要一个 bot client；从 context 取 aiocqhttp client
            clients = self.context.get_all_platforms()
            cqhttp_client = None
            for c in clients:
                if hasattr(c, "api"):
                    cqhttp_client = c
                    break
            if cqhttp_client is None:
                return

            raw = await cqhttp_client.api.call_action(
                "get_group_member_list", group_id=int(group_id)
            )
            if isinstance(raw, dict) and "data" in raw:
                raw = raw["data"]
            if isinstance(raw, list):
                members = raw
                for m in members:
                    uid = str(m.get("user_id"))
                    member_map[uid] = m.get("card") or m.get("nickname") or uid
        except Exception as e:
            logger.warning(f"[草限定] 获取群 {group_id} 成员失败: {e}")
            return

        if not members:
            return

        # 强制重新抽取今日限定
        today = datetime.now().strftime("%Y-%m-%d")
        chosen = secrets.choice(members)
        uid  = str(chosen.get("user_id"))
        name = chosen.get("card") or chosen.get("nickname") or uid
        self.xian_ding_data[group_id] = {"date": today, "uid": uid, "name": name}
        save_json(self.xian_ding_file, self.xian_ding_data)

        # 根据配置决定是否清空逃走计数
        if bool(self.config.get("xd_reset_escapes_on_announce", True)):
            if group_id in self._xd_escapes:
                self._xd_escapes[group_id] = {}

        # 构造消息：头像图片 + 文案
        avatar_url = f"https://q4.qlogo.cn/headimg_dl?dst_uin={uid}&spec=640"
        announce_text = self.DAILY_ANNOUNCE_TEXT.format(name=name)

        try:
            await cqhttp_client.api.call_action(
                "send_group_msg",
                group_id=int(group_id),
                message=[
                    {"type": "image", "data": {"file": avatar_url}},
                    {"type": "text",  "data": {"text": announce_text}},
                ],
            )
            logger.info(f"[草限定] 群 {group_id} 推送成功，今日限定：{name}({uid})")
        except Exception as e:
            logger.warning(f"[草限定] 群 {group_id} 发送消息失败: {e}")

    # ============================================================
    # 临时调试指令（确认管理员匹配后可删除）
    # ============================================================

    @filter.command(CMD_DEBUG)
    async def cao_debug(self, event: AstrMessageEvent):
        try:
            admins = self._get_bot_admins()
        except Exception as e:
            admins = [f"获取失败: {e}"]

        sender_id = str(event.get_sender_id())
        try:
            origin = str(event.unified_msg_origin)
        except Exception:
            origin = "不可用"

        lines = [
            f"sender_id: {sender_id}",
            f"unified_msg_origin: {origin}",
            f"admins列表({len(admins)}项):",
        ]
        for a in admins:
            lines.append(f"  - {repr(a)}")
        lines.append(f"_is_admin结果: {self._is_admin(event)}")
        yield event.plain_result("\n".join(lines))

    # ============================================================
    # 管理员判断
    # ============================================================

    def _get_bot_admins(self) -> list:
        """从 AstrBot 配置文件直接读取管理员列表。"""
        import glob as _glob
        candidates = []
        try:
            import astrbot
            _d = os.path.dirname(os.path.dirname(astrbot.__file__))
            candidates.append(os.path.join(_d, "data", "cmd_config.json"))
        except Exception:
            pass
        candidates += _glob.glob("/home/**/AstrBot/data/cmd_config.json", recursive=True)
        candidates += ["/AstrBot/data/cmd_config.json"]
        for candidate in list(dict.fromkeys(candidates)):
            if os.path.exists(candidate):
                try:
                    with open(candidate, "r", encoding="utf-8-sig") as f:
                        d = json.load(f)
                    result = d.get("admins_id") or d.get("admins") or []
                    if result:
                        return [str(a) for a in result]
                except Exception:
                    pass
        try:
            result = (
                getattr(self.context.config_helper, "admins_id", None)
                or getattr(self.context.config_helper, "admins", None)
                or []
            )
            return [str(a) for a in result]
        except Exception:
            return []

    def _is_admin(self, event: AstrMessageEvent) -> bool:
        """
        判断发送者是否为 bot 管理员。
        AstrBot 的 admins 列表里可能存：纯 QQ 号、或带平台前缀的 unified_id（如 aiocqhttp:123456）。
        这里同时匹配两种格式，只要有一种命中就返回 True。
        """
        try:
            admins = self._get_bot_admins()
        except Exception:
            admins = []

        sender_id = str(event.get_sender_id())
        # unified_msg_origin 形如 "aiocqhttp:GroupMessage:123456"，末段是 qq
        try:
            origin = str(event.unified_msg_origin)  # 可能不存在
        except Exception:
            origin = ""

        for a in admins:
            a_str = str(a).strip()
            # 直接匹配纯 QQ 号
            if a_str == sender_id:
                return True
            # 匹配带前缀格式，取最后一段
            if ":" in a_str and a_str.split(":")[-1] == sender_id:
                return True
            # 匹配 unified_msg_origin 整体
            if a_str == origin:
                return True
        return False

    # ============================================================
    # OneBot 辅助
    # ============================================================

    def _auto_withdraw_enabled(self) -> bool:
        return bool(self.config.get("auto_withdraw_enabled", False))

    def _auto_withdraw_delay_seconds(self) -> int:
        return int(self.config.get("auto_withdraw_delay_seconds", 5))

    def _can_onebot_withdraw(self, event: AstrMessageEvent) -> bool:
        if not self._auto_withdraw_enabled():
            return False
        return (
            event.get_platform_name() == "aiocqhttp"
            and isinstance(event, AiocqhttpMessageEvent)
        )

    async def _send_onebot_message(
        self, event: AstrMessageEvent, *, message: list[dict]
    ) -> object:
        assert isinstance(event, AiocqhttpMessageEvent)
        group_id = event.get_group_id()
        if group_id:
            resp = await event.bot.api.call_action(
                "send_group_msg", group_id=int(group_id), message=message
            )
        else:
            resp = await event.bot.api.call_action(
                "send_private_msg",
                user_id=int(event.get_sender_id()),
                message=message,
            )
        message_id = extract_message_id(resp)
        return message_id

    def _schedule_onebot_delete_msg(self, client, *, message_id: object) -> None:
        delay = self._auto_withdraw_delay_seconds()

        async def _runner():
            await asyncio.sleep(delay)
            try:
                await client.api.call_action("delete_msg", message_id=message_id)
            except Exception as e:
                logger.warning(f"自动撤回失败: {e}")

        task = asyncio.create_task(_runner())
        self._withdraw_tasks.add(task)
        task.add_done_callback(self._withdraw_tasks.discard)

    # ============================================================
    # 草群友 数据辅助
    # ============================================================

    def _ensure_today_cao_records(self) -> None:
        today = datetime.now().strftime("%Y-%m-%d")
        if self.cao_records.get("date") != today:
            self.cao_records = {"date": today, "groups": {}}

    def _get_cao_group_records(self, group_id: str) -> list:
        self._ensure_today_cao_records()
        if group_id not in self.cao_records["groups"]:
            self.cao_records["groups"][group_id] = {"records": []}
        return self.cao_records["groups"][group_id]["records"]

    def _clean_cao_stats(self) -> None:
        now = time.time()
        thirty_days = 30 * 24 * 3600
        new_stats = {}
        for gid, users in self.cao_stats.items():
            new_users = {}
            for uid, ts_list in users.items():
                valid = [ts for ts in ts_list if now - ts < thirty_days]
                if valid:
                    new_users[uid] = valid
            if new_users:
                new_stats[gid] = new_users
        self.cao_stats = new_stats
        save_json(self.cao_stats_file, self.cao_stats)

    def _record_qy_body(self, group_id: str, target_id: str, ml: float) -> None:
        """记录被草群友体内注入量（月度统计，以被草的群友为维度）"""
        today = datetime.now()
        reset_date = today.strftime("%Y-%m-01")

        if group_id not in self.qy_body_data:
            self.qy_body_data[group_id] = {
                "total_ml": 0.0, "count": 0,
                "last_reset": reset_date, "users": {}
            }

        gdata = self.qy_body_data[group_id]

        if gdata.get("last_reset", "") != reset_date:
            gdata["total_ml"] = 0.0
            gdata["count"] = 0
            gdata["users"] = {}
            gdata["last_reset"] = reset_date

        gdata["total_ml"] = round(gdata.get("total_ml", 0.0) + ml, 1)
        gdata["count"] = gdata.get("count", 0) + 1

        if target_id not in gdata["users"]:
            gdata["users"][target_id] = {"count": 0, "ml": 0.0}
        gdata["users"][target_id]["count"] += 1
        gdata["users"][target_id]["ml"] = round(gdata["users"][target_id]["ml"] + ml, 1)

        save_json(self.qy_body_file, self.qy_body_data)

    def _record_qy_battle_attacker(self, group_id: str, attacker_id: str, ml: float) -> None:
        """记录主动草人的注入量（30天滚动，以攻方为维度）"""
        now = time.time()
        if group_id not in self.qy_battle_data:
            self.qy_battle_data[group_id] = {"attackers": {}, "victims": {}}
        gdata = self.qy_battle_data[group_id]
        if "attackers" not in gdata:
            gdata["attackers"] = {}
        if attacker_id not in gdata["attackers"]:
            gdata["attackers"][attacker_id] = {"records": []}
        gdata["attackers"][attacker_id]["records"].append({"ts": now, "ml": ml})
        self._clean_qy_battle()
        save_json(self.qy_battle_file, self.qy_battle_data)

    def _record_qy_battle_victim(
        self, group_id: str, victim_id: str, ml: float, attacker_id: str = ""
    ) -> None:
        """记录被草次数和注入量（30天滚动，以被草方为维度），同时记录攻击者id用于来源分析"""
        now = time.time()
        if group_id not in self.qy_battle_data:
            self.qy_battle_data[group_id] = {"attackers": {}, "victims": {}}
        gdata = self.qy_battle_data[group_id]
        if "victims" not in gdata:
            gdata["victims"] = {}
        if victim_id not in gdata["victims"]:
            gdata["victims"][victim_id] = {"records": []}
        gdata["victims"][victim_id]["records"].append({
            "ts": now, "ml": ml, "attacker_id": attacker_id
        })
        self._clean_qy_battle()
        save_json(self.qy_battle_file, self.qy_battle_data)

    def _clean_qy_battle(self) -> None:
        now = time.time()
        cutoff = 30 * 24 * 3600
        new_data = {}
        for gid, gdata in self.qy_battle_data.items():
            new_gdata = {}
            for key in ("attackers", "victims"):
                new_sub = {}
                for uid, udata in gdata.get(key, {}).items():
                    valid = [r for r in udata.get("records", []) if now - r["ts"] < cutoff]
                    if valid:
                        new_sub[uid] = {"records": valid}
                if new_sub:
                    new_gdata[key] = new_sub
            if new_gdata:
                new_data[gid] = new_gdata
        self.qy_battle_data = new_data

    def _record_outside(self, group_id: str, user_id: str, ml: float) -> None:
        now = time.time()
        if group_id not in self.outside_stats_data:
            self.outside_stats_data[group_id] = {}
        gdata = self.outside_stats_data[group_id]
        if user_id not in gdata:
            gdata[user_id] = {"records": []}
        gdata[user_id]["records"].append({"ts": now, "ml": ml})
        self._clean_outside()
        save_json(self.outside_stats_file, self.outside_stats_data)
        save_json(self.xian_ding_file,      self.xian_ding_data)

    def _clean_outside(self) -> None:
        now = time.time()
        cutoff = 30 * 24 * 3600
        new_data = {}
        for gid, users in self.outside_stats_data.items():
            new_users = {}
            for uid, udata in users.items():
                valid = [r for r in udata.get("records", []) if now - r["ts"] < cutoff]
                if valid:
                    new_users[uid] = {"records": valid}
            if new_users:
                new_data[gid] = new_users
        self.outside_stats_data = new_data

    # ============================================================
    # /草群友
    # ============================================================

    @filter.command(CMD_CAOQUNYOU)
    async def caoqunyou(self, event: AstrMessageEvent):
        async for result in self._cmd_caoqunyou(event):
            yield result

    async def _cmd_caoqunyou(self, event: AstrMessageEvent):
        if event.is_private_chat():
            yield event.plain_result("此功能仅在群聊中可用哦~")
            return

        group_id = str(event.get_group_id())
        if not is_allowed_group(group_id, self.config):
            return

        user_id   = str(event.get_sender_id())
        user_name = event.get_sender_name() or f"用户({user_id})"
        allow_self = bool(self.config.get("allow_self_cao", False))

        # 获取群成员列表
        members = []
        member_map = {}
        try:
            if event.get_platform_name() == "aiocqhttp":
                assert isinstance(event, AiocqhttpMessageEvent)
                raw_members = await event.bot.api.call_action(
                    "get_group_member_list", group_id=int(group_id)
                )
                if isinstance(raw_members, dict) and "data" in raw_members:
                    raw_members = raw_members["data"]
                if isinstance(raw_members, list):
                    members = raw_members
                    for m in members:
                        uid = str(m.get("user_id"))
                        member_map[uid] = m.get("card") or m.get("nickname") or uid
                    user_name = member_map.get(user_id, user_name)
        except Exception:
            pass

        # 解析 @目标
        target_id: str | None = None
        for seg in event.get_messages():
            if hasattr(seg, "qq"):
                t = str(seg.qq)
                if t != user_id:
                    target_id = t
                    break

        # 若没有指定目标，从群员中随机选一个（排除自己）
        if target_id is None:
            candidates = [str(m.get("user_id")) for m in members
                          if str(m.get("user_id")) != user_id]
            if not candidates:
                yield event.plain_result("群里就你一个人，草谁？")
                return
            target_id = secrets.choice(candidates)

        # 禁止自草检查
        if not allow_self and target_id == user_id:
            yield event.plain_result("不可以草自己哦~")
            return

        target_name = member_map.get(target_id, f"用户({target_id})")

        # 每日次数限制
        today = datetime.now().strftime("%Y-%m-%d")
        if self.cao_daily.get("date") != today:
            self.cao_daily = {"date": today, "groups": {}}
        daily_limit = int(self.config.get("cao_daily_limit", 5))
        used_today  = self.cao_daily["groups"].get(group_id, {}).get(user_id, 0)
        if used_today >= daily_limit:
            yield event.plain_result(
                f"你今天已经草了 {daily_limit} 次群友了，群友们受不了啦，明天再来吧~"
            )
            return

        # 概率判定
        cao_prob = float(self.config.get("cao_probability", 30))
        cao_prob = max(0.0, min(100.0, cao_prob))

        if _secrets_roll() >= cao_prob / 100.0:
            fake_pct = secrets.randbelow(99) + 1

            fancao_base = float(self.config.get("fancao_probability", 50))
            fancao_base = max(0.0, min(100.0, fancao_base))

            if fancao_base > 0:
                times_today    = len(self._get_cao_group_records(group_id))
                # 仇恨值：目标被该user草的次数（30天）
                user_30d_count = len(self.cao_stats.get(group_id, {}).get(user_id, []))

                p_fancao = _calc_fancao_prob(fake_pct, times_today, user_30d_count, fancao_base)

                if _secrets_roll() < p_fancao:
                    # 反草成功：目标反草发起者
                    grudge = min(1.0, user_30d_count / 15.0)
                    if group_id not in self._fancao_pending:
                        self._fancao_pending[group_id] = {}
                    self._fancao_pending[group_id][user_id] = True
                    if group_id not in self._fancao_meta:
                        self._fancao_meta[group_id] = {}
                    # 反草：目标反草发起者（user_id），所以attacker=target, victim=user
                    self._fancao_meta[group_id][user_id] = {
                        "fake_pct":      fake_pct,
                        "grudge":        grudge,
                        "attacker_id":   target_id,    # 反草发起者（原目标）
                        "attacker_name": target_name,
                        "victim_id":     user_id,      # 被反草的人（原攻击者）
                        "victim_name":   user_name,
                    }

                    _fancao_comment = _pick_fancao_comment(self._fancao_comments, grudge)
                    text = (
                        f" {_fancao_comment or '反草成功！🔥'}\n"
                        f"【{target_name}】反草了【{user_name}】！\n\n"
                        f"请选择：回复【里面】或【外面】"
                    )
                    if self._can_onebot_withdraw(event):
                        message_id = await self._send_onebot_message(
                            event,
                            message=[
                                {"type": "at", "data": {"qq": user_id}},
                                {"type": "text", "data": {"text": text}},
                            ],
                        )
                        if message_id is not None:
                            self._schedule_onebot_delete_msg(event.bot, message_id=message_id)
                        return
                    yield event.chain_result([Comp.At(qq=user_id), Comp.Plain(text)])
                    return

            yield event.plain_result(f"在 {fake_pct}% 的时候被【{target_name}】逃走了~")
            return

        # 草成功！
        # 标记今日已用（+1）
        if group_id not in self.cao_daily["groups"]:
            self.cao_daily["groups"][group_id] = {}
        self.cao_daily["groups"][group_id][user_id] = used_today + 1
        save_json(self.cao_daily_file, self.cao_daily)

        # 统计记录（30天）
        if group_id not in self.cao_stats:
            self.cao_stats[group_id] = {}
        if user_id not in self.cao_stats[group_id]:
            self.cao_stats[group_id][user_id] = []
        self.cao_stats[group_id][user_id].append(time.time())
        self._clean_cao_stats()
        save_json(self.cao_stats_file, self.cao_stats)

        # 今日关系图记录（有向：attacker→target）
        group_cao_records = self._get_cao_group_records(group_id)
        group_cao_records.append({
            "attacker_id":   user_id,
            "attacker_name": user_name,
            "target_id":     target_id,
            "target_name":   target_name,
            "timestamp":     datetime.now().isoformat(),
        })
        save_json(self.cao_records_file, self.cao_records)

        # 进入等待内外选择状态
        if group_id not in self._cao_pending:
            self._cao_pending[group_id] = {}
        self._cao_pending[group_id][user_id] = {
            "target_id":   target_id,
            "target_name": target_name,
        }

        remaining = daily_limit - (used_today + 1)
        text = (
            f" 草群友成功！🎉\n【{user_name}】草了【{target_name}】！\n"
            f"今日剩余次数：{remaining} 次\n\n"
            f"请选择：回复【里面】或【外面】"
        )

        if self._can_onebot_withdraw(event):
            message_id = await self._send_onebot_message(
                event,
                message=[
                    {"type": "at", "data": {"qq": user_id}},
                    {"type": "text", "data": {"text": text}},
                ],
            )
            if message_id is not None:
                self._schedule_onebot_delete_msg(event.bot, message_id=message_id)
            return

        yield event.chain_result([Comp.At(qq=user_id), Comp.Plain(text)])

    # ============================================================
    # 内外选择监听（草群友成功后）
    # ============================================================

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def caoqunyou_choice_listener(self, event: AstrMessageEvent):
        if event.is_private_chat():
            return

        group_id = str(event.get_group_id())
        user_id  = str(event.get_sender_id())

        pending_meta = self._cao_pending.get(group_id, {}).get(user_id)
        if not pending_meta:
            return

        msg = event.message_str.strip()
        if msg not in ("里面", "外面"):
            return

        del self._cao_pending[group_id][user_id]

        user_name   = event.get_sender_name() or f"用户({user_id})"
        target_id   = pending_meta["target_id"]
        target_name = pending_meta["target_name"]

        if msg == "里面":
            ml    = _roll_injection_ml(fake_pct=None, grudge=0.0)
            grade = _ml_grade(ml)
            # 体内：记录被草的目标
            self._record_qy_body(group_id, target_id, ml)
            # 战绩：记录攻方（user_id）的注入量 + 记录被草方（target_id）
            self._record_qy_battle_attacker(group_id, user_id, ml)
            self._record_qy_battle_victim(group_id, target_id, ml, attacker_id=user_id)
            _body_comment = _pick_comment(self._body_comments, ml)
            text = (
                f" 【{user_name}】选择了射在【{target_name}】里面！\n"
                f"本次注入量：{_fmt_ml(ml)}　评分：{grade}\n"
                + (_body_comment if _body_comment else f"【{target_name}】感觉热热的~")
            )
        else:
            _outside_ml = _roll_injection_ml(fake_pct=None, grudge=0.0)
            self._record_outside(group_id, user_id, _outside_ml)
            text = (
                f" 【{user_name}】选择了射在【{target_name}】外面！✨\n"
                f"【{target_name}】松了一口气~"
            )

        if self._can_onebot_withdraw(event):
            await self._send_onebot_message(
                event,
                message=[
                    {"type": "at", "data": {"qq": user_id}},
                    {"type": "text", "data": {"text": text}},
                ],
            )
            event.stop_event()
            return

        yield event.chain_result([Comp.At(qq=user_id), Comp.Plain(text)])
        event.stop_event()

    # ============================================================
    # 反草内外选择监听
    # ============================================================

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def fancao_choice_listener(self, event: AstrMessageEvent):
        if event.is_private_chat():
            return

        group_id = str(event.get_group_id())
        user_id  = str(event.get_sender_id())

        if not self._fancao_pending.get(group_id, {}).get(user_id):
            return

        msg = event.message_str.strip()
        if msg not in ("里面", "外面"):
            return

        del self._fancao_pending[group_id][user_id]
        meta        = self._fancao_meta.get(group_id, {}).pop(user_id, {})
        f_pct       = meta.get("fake_pct", 50)
        grudge      = meta.get("grudge", 0.0)
        attacker_id   = meta.get("attacker_id", "")   # 反草方（原目标）
        attacker_name = meta.get("attacker_name", "群友")
        victim_id     = meta.get("victim_id", user_id) # 被反草方（原攻击者）
        victim_name   = meta.get("victim_name", "用户")

        if msg == "里面":
            ml    = _roll_injection_ml(fake_pct=f_pct, grudge=grudge)
            grade = _ml_grade(ml)
            # 体内：记录被反草的人（原攻击者）
            self._record_qy_body(group_id, victim_id, ml)
            # 战绩：attacker（反草方）的注入量 + victim（被反草方）被草次数
            self._record_qy_battle_attacker(group_id, attacker_id, ml)
            self._record_qy_battle_victim(group_id, victim_id, ml, attacker_id=attacker_id)
            _battle_comment = _pick_comment(self._battle_comments, ml)
            text = (
                f" 【{victim_name}】选择了让【{attacker_name}】射在里面！\n"
                f"【{attacker_name}】本次注入量：{_fmt_ml(ml)}　评级：{grade}\n"
                + (_battle_comment if _battle_comment else f"【{victim_name}】感觉热热的~")
            )
        else:
            _outside_ml = _roll_injection_ml(fake_pct=f_pct, grudge=grudge)
            self._record_outside(group_id, victim_id, _outside_ml)
            text = (
                f" 【{victim_name}】选择了让【{attacker_name}】射在外面！✨\n"
                f"【{attacker_name}】松了一口气~"
            )

        if self._can_onebot_withdraw(event):
            await self._send_onebot_message(
                event,
                message=[
                    {"type": "at", "data": {"qq": user_id}},
                    {"type": "text", "data": {"text": text}},
                ],
            )
            event.stop_event()
            return

        yield event.chain_result([Comp.At(qq=user_id), Comp.Plain(text)])
        event.stop_event()

    # ============================================================
    # /群友体内
    # ============================================================

    @filter.command(CMD_QY_BODY)
    async def qy_body(self, event: AstrMessageEvent):
        async for result in self._cmd_qy_body(event):
            yield result

    async def _cmd_qy_body(self, event: AstrMessageEvent):
        if event.is_private_chat():
            yield event.plain_result("此功能仅在群聊中可用哦~")
            return

        group_id = str(event.get_group_id())
        if not is_allowed_group(group_id, self.config):
            return

        today = datetime.now()
        reset_date = today.strftime("%Y-%m-01")

        gdata = self.qy_body_data.get(group_id, {})

        if gdata.get("last_reset", "") != reset_date:
            gdata = {"total_ml": 0.0, "count": 0, "last_reset": reset_date, "users": {}}
            self.qy_body_data[group_id] = gdata
            save_json(self.qy_body_file, self.qy_body_data)

        total_ml    = gdata.get("total_ml", 0.0)
        total_count = gdata.get("count", 0)
        users_data  = gdata.get("users", {})

        if today.month == 12:
            next_reset = datetime(today.year + 1, 1, 1)
        else:
            next_reset = datetime(today.year, today.month + 1, 1)
        delta      = next_reset - today
        days_left  = delta.days
        hours_left = delta.seconds // 3600

        user_map = {}
        group_name = "群聊"
        try:
            if event.get_platform_name() == "aiocqhttp":
                assert isinstance(event, AiocqhttpMessageEvent)
                info = await event.bot.api.call_action("get_group_info", group_id=int(group_id))
                if isinstance(info, dict) and "data" in info:
                    info = info["data"]
                group_name = info.get("group_name", "群聊")
                members = await event.bot.api.call_action(
                    "get_group_member_list", group_id=int(group_id)
                )
                if isinstance(members, dict) and "data" in members and isinstance(members["data"], list):
                    members = members["data"]
                for m in members:
                    uid = str(m.get("user_id"))
                    user_map[uid] = m.get("card") or m.get("nickname") or uid
        except Exception:
            pass

        _raw_ranking = sorted(
            [
                {
                    "uid":    uid,
                    "name":   user_map.get(uid, f"用户({uid})"),
                    "count":  d["count"],
                    "_ml_raw": d["ml"],
                }
                for uid, d in users_data.items()
            ],
            key=lambda x: x["_ml_raw"],
            reverse=True,
        )
        ranking = [
            {**r, "ml": _fmt_ml(r["_ml_raw"])} for r in _raw_ranking
        ][:10]

        import tempfile
        tmp_path = tempfile.mktemp(suffix=".png")
        try:
            await _render_qy_body(
                group_name=group_name,
                total_ml_str=_fmt_ml(total_ml),
                total_count=total_count,
                reset_date=reset_date,
                days_left=days_left,
                hours_left=hours_left,
                ranking=ranking,
                out_path=tmp_path,
                cache_dir=os.path.join(self.curr_dir, "avatar_cache"),
                titles_path=os.path.join(self.curr_dir, "qy_body_titles.json"),
            )
            yield event.image_result(tmp_path)
        except Exception as e:
            logger.error(f"渲染群友体内失败: {e}")
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    # ============================================================
    # /群友战绩
    # ============================================================

    @filter.command(CMD_QY_BATTLE)
    async def qy_battle(self, event: AstrMessageEvent):
        async for result in self._cmd_qy_battle(event):
            yield result

    async def _cmd_qy_battle(self, event: AstrMessageEvent):
        if event.is_private_chat():
            yield event.plain_result("此功能仅在群聊中可用哦~")
            return

        group_id = str(event.get_group_id())
        if not is_allowed_group(group_id, self.config):
            return

        self._clean_qy_battle()
        gdata = self.qy_battle_data.get(group_id, {})

        if not gdata:
            yield event.plain_result("近30天还没有人草过群友，大家都很守规矩呢~")
            return

        user_map = {}
        try:
            if event.get_platform_name() == "aiocqhttp":
                assert isinstance(event, AiocqhttpMessageEvent)
                members = await event.bot.api.call_action(
                    "get_group_member_list", group_id=int(group_id)
                )
                if isinstance(members, dict) and "data" in members and isinstance(members["data"], list):
                    members = members["data"]
                for m in members:
                    uid = str(m.get("user_id"))
                    user_map[uid] = m.get("card") or m.get("nickname") or uid
        except Exception:
            pass

        # 草人总量榜（攻方注入总量）
        attacker_data = gdata.get("attackers", {})
        ranking_by_ml = []
        for uid, udata in attacker_data.items():
            records  = udata.get("records", [])
            total_ml = sum(r["ml"] for r in records)
            count    = len(records)
            ranking_by_ml.append({
                "uid":     uid,
                "name":    user_map.get(uid, f"用户({uid})"),
                "count":   count,
                "_ml_raw": total_ml,
                "ml":      _fmt_ml(total_ml),
            })
        ranking_by_ml = sorted(ranking_by_ml, key=lambda x: x["_ml_raw"], reverse=True)[:10]

        # 被草次数榜（受方被草次数）
        victim_data = gdata.get("victims", {})
        ranking_by_count = []
        for uid, udata in victim_data.items():
            records = udata.get("records", [])
            count   = len(records)
            total_ml = sum(r["ml"] for r in records)
            ranking_by_count.append({
                "uid":     uid,
                "name":    user_map.get(uid, f"用户({uid})"),
                "count":   count,
                "_ml_raw": total_ml,
                "ml":      _fmt_ml(total_ml),
            })
        ranking_by_count = sorted(ranking_by_count, key=lambda x: x["count"], reverse=True)[:10]

        import tempfile
        tmp_path = tempfile.mktemp(suffix=".png")
        try:
            await _render_qy_battle(
                ranking_by_ml=ranking_by_ml,
                ranking_by_count=ranking_by_count,
                out_path=tmp_path,
                cache_dir=os.path.join(self.curr_dir, "avatar_cache"),
                titles_path=os.path.join(self.curr_dir, "qy_battle_titles.json"),
            )
            yield event.image_result(tmp_path)
        except Exception as e:
            logger.error(f"渲染群友战绩失败: {e}")
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    # ============================================================
    # /杂鱼排行
    # ============================================================

    @filter.command(CMD_OUTSIDE_RANK)
    async def outside_rank(self, event: AstrMessageEvent):
        async for result in self._cmd_outside_rank(event):
            yield result

    async def _cmd_outside_rank(self, event: AstrMessageEvent):
        if event.is_private_chat():
            yield event.plain_result("此功能仅在群聊中可用哦~")
            return

        group_id = str(event.get_group_id())
        if not is_allowed_group(group_id, self.config):
            return

        self._clean_outside()
        gdata = self.outside_stats_data.get(group_id, {})

        if not gdata:
            yield event.plain_result("近30天没有人选择外面，大家都很勇敢~")
            return

        user_map = {}
        try:
            if event.get_platform_name() == "aiocqhttp":
                assert isinstance(event, AiocqhttpMessageEvent)
                members = await event.bot.api.call_action(
                    "get_group_member_list", group_id=int(group_id)
                )
                if isinstance(members, dict) and "data" in members and isinstance(members["data"], list):
                    members = members["data"]
                for m in members:
                    uid = str(m.get("user_id"))
                    user_map[uid] = m.get("card") or m.get("nickname") or uid
        except Exception:
            pass

        ranking = []
        for uid, udata in gdata.items():
            records  = udata.get("records", [])
            total_ml = sum(r["ml"] for r in records)
            count    = len(records)
            ranking.append({
                "uid":     uid,
                "name":    user_map.get(uid, f"用户({uid})"),
                "count":   count,
                "_ml_raw": total_ml,
                "ml":      _fmt_ml(total_ml),
            })

        ranking_by_count = sorted(ranking, key=lambda x: x["count"],   reverse=True)[:10]
        ranking_by_ml    = sorted(ranking, key=lambda x: x["_ml_raw"], reverse=True)[:10]

        import tempfile
        tmp_path = tempfile.mktemp(suffix=".png")
        try:
            await _render_outside_rank(
                nj_qq="",
                nj_name="杂鱼榜",
                ranking_by_count=ranking_by_count,
                ranking_by_ml=ranking_by_ml,
                out_path=tmp_path,
                cache_dir=os.path.join(self.curr_dir, "avatar_cache"),
                titles_path=os.path.join(self.curr_dir, "outside_titles.json"),
            )
            yield event.image_result(tmp_path)
        except Exception as e:
            logger.error(f"渲染杂鱼排行失败: {e}")
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    # ============================================================
    # /重置草群友
    # ============================================================

    @filter.command(CMD_RESET_CAOQUNYOU)
    async def reset_caoqunyou(self, event: AstrMessageEvent):
        async for result in self._cmd_reset_caoqunyou(event):
            yield result

    async def _cmd_reset_caoqunyou(self, event: AstrMessageEvent):
        if event.is_private_chat():
            yield event.plain_result("此功能仅在群聊中可用哦~")
            return

        group_id = str(event.get_group_id())
        if not is_allowed_group(group_id, self.config):
            return

        user_id = str(event.get_sender_id())
        today   = datetime.now().strftime("%Y-%m-%d")

        is_admin = self._is_admin(event)

        at_target: str | None = None
        reset_all = False
        raw = event.message_str.strip().removeprefix(CMD_RESET_CAOQUNYOU).strip()
        if raw == "全员":
            reset_all = True
        else:
            for seg in event.get_messages():
                if hasattr(seg, "qq") and str(seg.qq) != user_id:
                    at_target = str(seg.qq)
                    break

        if self.cao_daily.get("date") != today:
            self.cao_daily = {"date": today, "groups": {}}

        # 管理员逻辑
        if is_admin:
            if reset_all:
                self.cao_daily["groups"][group_id] = {}
                save_json(self.cao_daily_file, self.cao_daily)
                yield event.plain_result("已重置本群所有人今日草群友次数~")
                return

            target_id = at_target or user_id
            if group_id in self.cao_daily["groups"]:
                self.cao_daily["groups"][group_id].pop(target_id, None)
            save_json(self.cao_daily_file, self.cao_daily)
            if target_id == user_id:
                yield event.plain_result("已重置你今日草群友的次数~")
            else:
                yield event.plain_result(f"已重置 {target_id} 今日草群友的次数~")
            return

        # 普通用户逻辑
        if at_target or reset_all:
            yield event.plain_result("你没有权限重置他人的次数哦~")
            return

        max_attempts = int(self.config.get("reset_daily_attempts", 3))
        day_attempts = self._reset_attempts.setdefault(today, {}).setdefault(group_id, {})
        used = day_attempts.get(user_id, 0)

        if used >= max_attempts:
            yield event.plain_result(f"你今天已经尝试了 {max_attempts} 次，机会用完了~")
            return

        day_attempts[user_id] = used + 1
        remaining = max_attempts - used - 1

        cao_prob = float(self.config.get("cao_probability", 30))
        cao_prob = max(0.0, min(100.0, cao_prob))

        if _secrets_roll() < cao_prob / 100.0:
            if group_id in self.cao_daily["groups"]:
                self.cao_daily["groups"][group_id].pop(user_id, None)
            save_json(self.cao_daily_file, self.cao_daily)
            yield event.plain_result(
                f"重置成功！今日草群友次数已清零~\n"
                f"（今日剩余尝试次数：{remaining}）"
            )
        else:
            yield event.plain_result(
                f"重置失败，群友们不配合你~\n"
                f"（今日剩余尝试次数：{remaining}）"
            )

    # ============================================================
    # /草群友排行
    # ============================================================

    @filter.command(CMD_CAOQUNYOU_RANKING)
    async def caoqunyou_ranking(self, event: AstrMessageEvent):
        async for result in self._cmd_caoqunyou_ranking(event):
            yield result

    async def _cmd_caoqunyou_ranking(self, event: AstrMessageEvent):
        if event.is_private_chat():
            yield event.plain_result("私聊看不了榜单哦~")
            return

        group_id = str(event.get_group_id())
        self._clean_cao_stats()

        group_data = self.cao_stats.get(group_id, {})
        if not group_data:
            yield event.plain_result("本群近30天还没有人草过群友，大家都很守规矩呢。")
            return

        user_map = {}
        try:
            if event.get_platform_name() == "aiocqhttp":
                members = await event.bot.api.call_action(
                    "get_group_member_list", group_id=int(group_id)
                )
                if isinstance(members, dict) and "data" in members:
                    members = members["data"]
                for m in members:
                    uid = str(m.get("user_id"))
                    user_map[uid] = m.get("card") or m.get("nickname") or uid
        except Exception:
            pass

        sorted_list = sorted(
            [{"uid": uid, "name": user_map.get(uid, f"用户({uid})"), "count": len(ts_list)}
             for uid, ts_list in group_data.items()],
            key=lambda x: x["count"], reverse=True
        )[:10]

        current_rank = 1
        for i, user in enumerate(sorted_list):
            if i > 0 and user["count"] < sorted_list[i - 1]["count"]:
                current_rank = i + 1
            user["rank"] = current_rank

        template_path = os.path.join(self.curr_dir, "caoqunyou_ranking.html")
        if not os.path.exists(template_path):
            yield event.plain_result("错误：找不到排行模板 caoqunyou_ranking.html")
            return

        with open(template_path, "r", encoding="utf-8") as f:
            template_content = f.read()

        header_h, item_h, footer_h, rank_width = 100, 62, 50, 400
        sub_h = 32
        dynamic_height = header_h + sub_h + (len(sorted_list) * item_h) + footer_h

        try:
            url = await self.html_render(
                template_content,
                {
                    "group_id": group_id,
                    "ranking": sorted_list,
                    "title": "🔥 草群友月榜 🔥",
                },
                options={
                    "type": "png", "quality": None, "full_page": False,
                    "clip": {"x": 0, "y": 0, "width": rank_width, "height": dynamic_height},
                    "scale": "device", "device_scale_factor_level": "ultra",
                },
            )
            yield event.image_result(url)
        except Exception as e:
            logger.error(f"渲染草群友排行失败: {e}")

    # ============================================================
    # /草群友关系图
    # ============================================================

    @filter.command(CMD_CAOQUNYOU_GRAPH)
    async def caoqunyou_graph(self, event: AstrMessageEvent):
        async for result in self._cmd_caoqunyou_graph(event):
            yield result

    async def _cmd_caoqunyou_graph(self, event: AstrMessageEvent):
        if event.is_private_chat():
            yield event.plain_result("此功能仅在群聊中可用哦~")
            return

        group_id = str(event.get_group_id())
        if not is_allowed_group(group_id, self.config):
            return

        self._ensure_today_cao_records()
        group_cao_records = self.cao_records.get("groups", {}).get(group_id, {}).get("records", [])

        if not group_cao_records:
            yield event.plain_result("今天还没有人草过群友哦~")
            return

        group_name = "未命名群聊"
        user_map = {}
        try:
            if event.get_platform_name() == "aiocqhttp":
                assert isinstance(event, AiocqhttpMessageEvent)
                info = await event.bot.api.call_action("get_group_info", group_id=int(group_id))
                if isinstance(info, dict) and "data" in info:
                    info = info["data"]
                group_name = info.get("group_name", "未命名群聊")

                members = await event.bot.api.call_action(
                    "get_group_member_list", group_id=int(group_id)
                )
                if isinstance(members, dict) and "data" in members:
                    members = members["data"]
                if isinstance(members, list):
                    for m in members:
                        uid = str(m.get("user_id"))
                        user_map[uid] = m.get("card") or m.get("nickname") or uid
        except Exception as e:
            logger.warning(f"获取群信息失败: {e}")

        vis_js_path = os.path.join(self.curr_dir, "vis-network.min.js")
        vis_js_content = ""
        if os.path.exists(vis_js_path):
            with open(vis_js_path, "r", encoding="utf-8") as f:
                vis_js_content = f.read()

        template_path = os.path.join(self.curr_dir, "caoqunyou_graph_template.html")
        if not os.path.exists(template_path):
            yield event.plain_result("错误：找不到模板文件 caoqunyou_graph_template.html")
            return

        with open(template_path, "r", encoding="utf-8") as f:
            graph_html = f.read()

        # 统计唯一节点数
        all_ids = set()
        for r in group_cao_records:
            all_ids.add(r.get("attacker_id", ""))
            all_ids.add(r.get("target_id", ""))
        node_count = len(all_ids)
        clip_width  = 1920
        clip_height = 1080 + max(0, node_count - 10) * 60
        iter_count  = self.config.get("iterations", 140)

        try:
            url = await self.html_render(
                graph_html,
                {
                    "vis_js_content": vis_js_content,
                    "group_id":       group_id,
                    "group_name":     group_name,
                    "user_map":       user_map,
                    "records":        group_cao_records,
                    "iterations":     iter_count,
                },
                options={
                    "type": "png", "quality": None, "scale": "device",
                    "clip": {"x": 0, "y": 0, "width": clip_width, "height": clip_height},
                    "full_page": False, "device_scale_factor_level": "ultra",
                },
            )
            yield event.image_result(url)
        except Exception as e:
            logger.error(f"渲染草群友关系图失败: {e}")


    # ============================================================
    # 限定群友 辅助
    # ============================================================

    def _get_xian_ding(self, group_id: str) -> dict | None:
        """返回今日限定信息 {uid, name}，今天已抽过则直接返回，否则返回 None"""
        today = datetime.now().strftime("%Y-%m-%d")
        entry = self.xian_ding_data.get(group_id)
        if entry and entry.get("date") == today:
            return entry
        return None

    async def _draw_xian_ding(
        self, group_id: str, members: list[dict], exclude_uid: str = ""
    ) -> dict | None:
        """为今天随机抽一个限定群友，排除 exclude_uid，结果写入 xian_ding_data 并保存"""
        today = datetime.now().strftime("%Y-%m-%d")
        candidates = [m for m in members if str(m.get("user_id")) != exclude_uid]
        if not candidates:
            return None
        chosen = secrets.choice(candidates)
        uid  = str(chosen.get("user_id"))
        name = chosen.get("card") or chosen.get("nickname") or uid
        self.xian_ding_data[group_id] = {"date": today, "uid": uid, "name": name}
        save_json(self.xian_ding_file, self.xian_ding_data)
        return self.xian_ding_data[group_id]

    def _xd_escape_count(self, group_id: str, user_id: str) -> int:
        """返回今天该用户草限定的逃走次数"""
        return self._xd_escapes.get(group_id, {}).get(user_id, 0)

    def _xd_add_escape(self, group_id: str, user_id: str) -> int:
        """逃走次数 +1，返回新值"""
        if group_id not in self._xd_escapes:
            self._xd_escapes[group_id] = {}
        cnt = self._xd_escapes[group_id].get(user_id, 0) + 1
        self._xd_escapes[group_id][user_id] = cnt
        return cnt

    def _xd_success_prob(self, group_id: str, user_id: str) -> float:
        """草限定成功概率：基础值 + 逃走次数×加成，不超过上限。"""
        raw_base = float(self.config.get("xd_base_probability", -1))
        if raw_base < 0:
            # -1 表示复用草群友概率
            base = float(self.config.get("cao_probability", 30))
        else:
            base = raw_base
        base    = max(0.0, min(100.0, base))
        bonus   = float(self.config.get("xd_escape_bonus", 5))
        max_p   = float(self.config.get("xd_max_probability", 95))
        escapes = self._xd_escape_count(group_id, user_id)
        return min(max_p, base + escapes * bonus)

    # ============================================================
    # /草限定
    # ============================================================

    @filter.command(CMD_CAO_XIAN_DING)
    async def cao_xian_ding(self, event: AstrMessageEvent):
        async for result in self._cmd_cao_xian_ding(event):
            yield result

    async def _cmd_cao_xian_ding(self, event: AstrMessageEvent):
        if event.is_private_chat():
            yield event.plain_result("此功能仅在群聊中可用哦~")
            return

        group_id  = str(event.get_group_id())
        if not is_allowed_group(group_id, self.config):
            return

        user_id   = str(event.get_sender_id())

        # 拉群成员
        members = []
        member_map = {}
        try:
            if event.get_platform_name() == "aiocqhttp":
                assert isinstance(event, AiocqhttpMessageEvent)
                raw = await event.bot.api.call_action(
                    "get_group_member_list", group_id=int(group_id)
                )
                if isinstance(raw, dict) and "data" in raw:
                    raw = raw["data"]
                if isinstance(raw, list):
                    members = raw
                    for m in members:
                        uid = str(m.get("user_id"))
                        member_map[uid] = m.get("card") or m.get("nickname") or uid
        except Exception:
            pass

        user_name = member_map.get(user_id, event.get_sender_name() or f"用户({user_id})")

        # 确保今天有限定群友（没有就现在抽）
        xd = self._get_xian_ding(group_id)
        if not xd:
            allow_self = bool(self.config.get("allow_self_cao", False))
            xd = await self._draw_xian_ding(
                group_id, members,
                exclude_uid="" if allow_self else user_id,
            )
        if not xd:
            yield event.plain_result("群里就你一个人，草谁？")
            return

        # 限定不能是自己（除非开了allow_self_cao）
        allow_self = bool(self.config.get("allow_self_cao", False))
        if not allow_self and xd["uid"] == user_id:
            # 重新抽一个排除自己的
            xd = await self._draw_xian_ding(group_id, members, exclude_uid=user_id)
            if not xd:
                yield event.plain_result("群里就你一个人，草谁？")
                return

        xd_uid  = xd["uid"]
        xd_name = member_map.get(xd_uid, xd["name"])

        # 计算本次成功概率
        prob     = self._xd_success_prob(group_id, user_id)
        escapes  = self._xd_escape_count(group_id, user_id)
        prob_tip = f"（当前概率：{prob:.0f}%"
        if escapes > 0:
            prob_tip += f"，已逃走 {escapes} 次"
        prob_tip += "）"

        # 掷骰
        if _secrets_roll() >= prob / 100.0:
            # 逃走，概率+5%
            new_escapes = self._xd_add_escape(group_id, user_id)
            _xd_base2 = float(self.config.get("xd_base_probability", -1))
            if _xd_base2 < 0:
                _xd_base2 = float(self.config.get("cao_probability", 30))
            _xd_bonus2 = float(self.config.get("xd_escape_bonus", 5))
            _xd_max2   = float(self.config.get("xd_max_probability", 95))
            new_prob   = min(_xd_max2, _xd_base2 + new_escapes * _xd_bonus2)
            fake_pct    = secrets.randbelow(99) + 1

            # 限定也可以反草
            fancao_base = float(self.config.get("fancao_probability", 50))
            if fancao_base > 0:
                times_today    = len(self._get_cao_group_records(group_id))
                user_30d_count = len(self.cao_stats.get(group_id, {}).get(user_id, []))
                p_fancao = _calc_fancao_prob(fake_pct, times_today, user_30d_count, fancao_base)
                if _secrets_roll() < p_fancao:
                    grudge = min(1.0, user_30d_count / 15.0)
                    if group_id not in self._fancao_pending:
                        self._fancao_pending[group_id] = {}
                    self._fancao_pending[group_id][user_id] = True
                    if group_id not in self._fancao_meta:
                        self._fancao_meta[group_id] = {}
                    self._fancao_meta[group_id][user_id] = {
                        "fake_pct":      fake_pct,
                        "grudge":        grudge,
                        "attacker_id":   xd_uid,
                        "attacker_name": xd_name,
                        "victim_id":     user_id,
                        "victim_name":   user_name,
                    }
                    _fc = _pick_fancao_comment(self._fancao_comments, grudge)
                    text = (
                        f" {_fc or '反草成功！🔥'}\n"
                        f"【限定·{xd_name}】反草了【{user_name}】！\n\n"
                        f"请选择：回复【里面】或【外面】"
                    )
                    if self._can_onebot_withdraw(event):
                        mid = await self._send_onebot_message(event, message=[
                            {"type": "at", "data": {"qq": user_id}},
                            {"type": "text", "data": {"text": text}},
                        ])
                        if mid: self._schedule_onebot_delete_msg(event.bot, message_id=mid)
                        return
                    yield event.chain_result([Comp.At(qq=user_id), Comp.Plain(text)])
                    return

            text = (
                f" 【限定·{xd_name}】在 {fake_pct}% 的时候逃走了~\n"
                f"下次成功概率提升至 {new_prob:.0f}%，继续加油！"
            )
            if self._can_onebot_withdraw(event):
                mid = await self._send_onebot_message(event, message=[
                    {"type": "at", "data": {"qq": user_id}},
                    {"type": "text", "data": {"text": text}},
                ])
                if mid: self._schedule_onebot_delete_msg(event.bot, message_id=mid)
                return
            yield event.chain_result([Comp.At(qq=user_id), Comp.Plain(text)])
            return

        # 草成功！（不受每日次数限制，但记入 cao_stats / cao_records / body / battle）
        if group_id not in self.cao_stats:
            self.cao_stats[group_id] = {}
        if user_id not in self.cao_stats[group_id]:
            self.cao_stats[group_id][user_id] = []
        self.cao_stats[group_id][user_id].append(time.time())
        self._clean_cao_stats()
        save_json(self.cao_stats_file, self.cao_stats)

        group_cao_records = self._get_cao_group_records(group_id)
        group_cao_records.append({
            "attacker_id":   user_id,
            "attacker_name": user_name,
            "target_id":     xd_uid,
            "target_name":   xd_name,
            "timestamp":     datetime.now().isoformat(),
            "is_xian_ding":  True,
        })
        save_json(self.cao_records_file, self.cao_records)

        # 进入内外选择
        if group_id not in self._cao_pending:
            self._cao_pending[group_id] = {}
        self._cao_pending[group_id][user_id] = {
            "target_id":   xd_uid,
            "target_name": xd_name,
        }

        text = (
            f" 草限定成功！🎉✨\n"
            f"【{user_name}】草了今日限定【{xd_name}】！\n"
            f"{prob_tip}\n\n"
            f"请选择：回复【里面】或【外面】"
        )
        if self._can_onebot_withdraw(event):
            mid = await self._send_onebot_message(event, message=[
                {"type": "at", "data": {"qq": user_id}},
                {"type": "text", "data": {"text": text}},
            ])
            if mid: self._schedule_onebot_delete_msg(event.bot, message_id=mid)
            return
        yield event.chain_result([Comp.At(qq=user_id), Comp.Plain(text)])

    # ============================================================
    # /今日限定
    # ============================================================

    @filter.command(CMD_XIAN_DING_INFO)
    async def xian_ding_info(self, event: AstrMessageEvent):
        async for result in self._cmd_xian_ding_info(event):
            yield result

    async def _cmd_xian_ding_info(self, event: AstrMessageEvent):
        if event.is_private_chat():
            yield event.plain_result("此功能仅在群聊中可用哦~")
            return

        group_id = str(event.get_group_id())
        if not is_allowed_group(group_id, self.config):
            return

        user_id = str(event.get_sender_id())

        # 拉群成员（用于显示最新昵称 + 抽限定兜底）
        members = []
        member_map = {}
        try:
            if event.get_platform_name() == "aiocqhttp":
                assert isinstance(event, AiocqhttpMessageEvent)
                raw = await event.bot.api.call_action(
                    "get_group_member_list", group_id=int(group_id)
                )
                if isinstance(raw, dict) and "data" in raw:
                    raw = raw["data"]
                if isinstance(raw, list):
                    members = raw
                    for m in members:
                        uid = str(m.get("user_id"))
                        member_map[uid] = m.get("card") or m.get("nickname") or uid
        except Exception:
            pass

        xd = self._get_xian_ding(group_id)
        if not xd:
            # 今天还没抽，现在抽
            allow_self = bool(self.config.get("allow_self_cao", False))
            xd = await self._draw_xian_ding(
                group_id, members, exclude_uid="" if allow_self else user_id
            )

        if not xd:
            yield event.plain_result("群里还没有其他人，无法抽取限定哦~")
            return

        xd_uid  = xd["uid"]
        xd_name = member_map.get(xd_uid, xd["name"])
        escapes = self._xd_escape_count(group_id, user_id)
        prob    = self._xd_success_prob(group_id, user_id)
        prob_str = f"{prob:.0f}%"
        if escapes > 0:
            prob_str += f"（已逃走 {escapes} 次，每次 +5%）"

        # ----------------------------------------------------------------
        # ★ /限定 查询文案 —— 在这里自定义
        # 可用变量：
        #   xd_name  = 限定群友昵称
        #   xd_uid   = 限定群友QQ号
        #   prob_str = 你当前的成功概率字符串
        # ----------------------------------------------------------------
        avatar_url = f"https://q4.qlogo.cn/headimg_dl?dst_uin={xd_uid}&spec=640"
        text = (
            f"今日限定：【{xd_name}】\n"
            f"草限定成功概率：{prob_str}"
            # 在这里追加你的文案
        )

        if event.get_platform_name() == "aiocqhttp" and isinstance(event, AiocqhttpMessageEvent):
            await event.bot.api.call_action(
                "send_group_msg",
                group_id=int(group_id),
                message=[
                    {"type": "image", "data": {"file": avatar_url}},
                    {"type": "text",  "data": {"text": text}},
                ],
            )
        else:
            yield event.plain_result(text)

    # ============================================================
    # /草全群
    # ============================================================

    @filter.command(CMD_CAO_QUAN_QUN)
    async def cao_quan_qun(self, event: AstrMessageEvent):
        async for result in self._cmd_cao_quan_qun(event):
            yield result

    async def _cmd_cao_quan_qun(self, event: AstrMessageEvent):
        if event.is_private_chat():
            yield event.plain_result("此功能仅在群聊中可用哦~")
            return

        group_id = str(event.get_group_id())
        if not is_allowed_group(group_id, self.config):
            return

        user_id   = str(event.get_sender_id())
        user_name = event.get_sender_name() or f"用户({user_id})"

        # ── 每日冷却检查 ────────────────────────────────────────
        today = datetime.now().strftime("%Y-%m-%d")
        gcd   = self.cao_quan_qun_cd.get(group_id, {})
        if gcd.get(user_id) == today:
            yield event.plain_result("你今天已经草过全群了，群友们需要休息，明天再来吧~")
            return

        # ── 拉群成员列表 ────────────────────────────────────────
        members    = []
        member_map = {}
        try:
            if event.get_platform_name() == "aiocqhttp":
                assert isinstance(event, AiocqhttpMessageEvent)
                raw = await event.bot.api.call_action(
                    "get_group_member_list", group_id=int(group_id)
                )
                if isinstance(raw, dict) and "data" in raw:
                    raw = raw["data"]
                if isinstance(raw, list):
                    members = raw
                    for m in members:
                        uid = str(m.get("user_id"))
                        member_map[uid] = m.get("card") or m.get("nickname") or uid
                    user_name = member_map.get(user_id, user_name)
        except Exception:
            pass

        allow_self = bool(self.config.get("allow_self_cao", False))
        candidates = [
            str(m.get("user_id")) for m in members
            if (allow_self or str(m.get("user_id")) != user_id)
        ]
        if not candidates:
            yield event.plain_result("群里没有其他人，草不了~")
            return

        # ── 标记冷却 ────────────────────────────────────────────
        if group_id not in self.cao_quan_qun_cd:
            self.cao_quan_qun_cd[group_id] = {}
        self.cao_quan_qun_cd[group_id][user_id] = today
        save_json(self.cao_quan_qun_cd_file, self.cao_quan_qun_cd)

        # ── 逐一判定 ────────────────────────────────────────────
        cao_prob    = float(self.config.get("cao_probability", 30))
        cao_prob    = max(0.0, min(100.0, cao_prob))
        fancao_base = float(self.config.get("fancao_probability", 50))
        fancao_base = max(0.0, min(100.0, fancao_base))

        success_count  = 0
        escaped_count  = 0
        fancaoed_count = 0

        for target_id in candidates:
            target_name = member_map.get(target_id, f"用户({target_id})")

            if _secrets_roll() >= cao_prob / 100.0:
                # 逃走或反草
                if fancao_base > 0:
                    fake_pct       = secrets.randbelow(99) + 1
                    times_today    = len(self._get_cao_group_records(group_id))
                    user_30d_count = len(self.cao_stats.get(group_id, {}).get(user_id, []))
                    p_fancao       = _calc_fancao_prob(fake_pct, times_today, user_30d_count, fancao_base)
                    if _secrets_roll() < p_fancao:
                        # 反草：默认里面，静默处理
                        grudge = min(1.0, user_30d_count / 15.0)
                        ml     = _roll_injection_ml(fake_pct=fake_pct, grudge=grudge)
                        self._record_qy_body(group_id, user_id, ml)
                        self._record_qy_battle_attacker(group_id, target_id, ml)
                        self._record_qy_battle_victim(group_id, user_id, ml, attacker_id=target_id)
                        fancaoed_count += 1
                        continue
                escaped_count += 1
            else:
                # 草成功：默认里面，静默处理
                ml = _roll_injection_ml(fake_pct=None, grudge=0.0)
                self._record_qy_body(group_id, target_id, ml)
                self._record_qy_battle_attacker(group_id, user_id, ml)
                self._record_qy_battle_victim(group_id, target_id, ml, attacker_id=user_id)

                # 也记录到 cao_stats / cao_records（不扣每日次数）
                if group_id not in self.cao_stats:
                    self.cao_stats[group_id] = {}
                if user_id not in self.cao_stats[group_id]:
                    self.cao_stats[group_id][user_id] = []
                self.cao_stats[group_id][user_id].append(time.time())
                group_cao_records = self._get_cao_group_records(group_id)
                group_cao_records.append({
                    "attacker_id":   user_id,
                    "attacker_name": user_name,
                    "target_id":     target_id,
                    "target_name":   target_name,
                    "timestamp":     datetime.now().isoformat(),
                })
                success_count += 1

        self._clean_cao_stats()
        save_json(self.cao_stats_file,  self.cao_stats)
        save_json(self.cao_records_file, self.cao_records)

        # ── 记录草全群日志（供「我的战绩」查询） ───────────────
        if group_id not in self.cao_quan_qun_log:
            self.cao_quan_qun_log[group_id] = {}
        if user_id not in self.cao_quan_qun_log[group_id]:
            self.cao_quan_qun_log[group_id][user_id] = []
        self.cao_quan_qun_log[group_id][user_id].append({
            "ts":       time.time(),
            "success":  success_count,
            "escaped":  escaped_count,
            "fancaoed": fancaoed_count,
        })
        # 只保留近30天
        cutoff = time.time() - 86400 * 30
        self.cao_quan_qun_log[group_id][user_id] = [
            r for r in self.cao_quan_qun_log[group_id][user_id]
            if r["ts"] >= cutoff
        ]
        save_json(self.cao_quan_qun_log_file, self.cao_quan_qun_log)

        # ── 汇总文本 ────────────────────────────────────────────
        total = len(candidates)
        lines = [f"【{user_name}】对全群 {total} 人发起了草群行动！"]
        lines.append(f"✅ 成功草了 {success_count} 人")
        lines.append(f"🏃 {escaped_count} 人逃走了")
        if fancaoed_count:
            lines.append(f"⚔️ 被 {fancaoed_count} 人反草了！")
        yield event.plain_result("\n".join(lines))

    # ============================================================
    # /我的战绩
    # ============================================================

    @filter.command(CMD_MY_BATTLE)
    async def my_battle(self, event: AstrMessageEvent):
        async for result in self._cmd_my_battle(event):
            yield result

    async def _cmd_my_battle(self, event: AstrMessageEvent):
        if event.is_private_chat():
            yield event.plain_result("此功能仅在群聊中可用哦~")
            return

        group_id = str(event.get_group_id())
        if not is_allowed_group(group_id, self.config):
            return

        sender_id = str(event.get_sender_id())
        # 支持 @ 查别人战绩
        target_id = sender_id
        for seg in event.get_messages():
            if hasattr(seg, "qq"):
                t = str(seg.qq)
                if t != sender_id:
                    target_id = t
                    break

        # 拉群成员
        user_map = {}
        try:
            if event.get_platform_name() == "aiocqhttp":
                assert isinstance(event, AiocqhttpMessageEvent)
                members = await event.bot.api.call_action(
                    "get_group_member_list", group_id=int(group_id)
                )
                if isinstance(members, dict) and "data" in members:
                    members = members["data"]
                for m in members:
                    uid = str(m.get("user_id"))
                    user_map[uid] = m.get("card") or m.get("nickname") or uid
        except Exception:
            pass

        target_name = user_map.get(target_id, f"用户({target_id})")

        # ── 近30天攻方记录 ──────────────────────────────────────
        self._clean_qy_battle()
        attacker_records = (
            self.qy_battle_data
            .get(group_id, {})
            .get("attackers", {})
            .get(target_id, {})
            .get("records", [])
        )

        # ── 被反草次数（作为victim，attacker != self） ──────────
        # 取 victim 记录中 attacker_id != target_id 的数量（即被他人草）
        victim_records_all = (
            self.qy_battle_data
            .get(group_id, {})
            .get("victims", {})
            .get(target_id, {})
            .get("records", [])
        )
        # 反草：在攻方视角下，我作为victim
        fancao_count = len(victim_records_all)

        # ── 草全群次数（近30天） ────────────────────────────────
        cutoff = time.time() - 86400 * 30
        cqq_logs = self.cao_quan_qun_log.get(group_id, {}).get(target_id, [])
        caoquanqun_count = sum(1 for r in cqq_logs if r.get("ts", 0) >= cutoff)

        # ── 打倒最多的目标 Top3 ─────────────────────────────────
        # attacker 记录只存 {ts, ml}，没有 victim_id；
        # 需要从 victims 侧反查：遍历所有 victim，找 attacker_id == target_id 的记录
        victim_agg: dict[str, dict] = {}
        all_victims = self.qy_battle_data.get(group_id, {}).get("victims", {})
        for vid, vdata in all_victims.items():
            for r in vdata.get("records", []):
                if r.get("attacker_id", "") == target_id:
                    if vid not in victim_agg:
                        victim_agg[vid] = {
                            "uid":      vid,
                            "name":     user_map.get(vid, f"用户({vid})"),
                            "count":    0,
                            "total_ml": 0.0,
                        }
                    victim_agg[vid]["count"]    += 1
                    victim_agg[vid]["total_ml"] = round(
                        victim_agg[vid]["total_ml"] + r.get("ml", 0.0), 1
                    )

        top_victims = sorted(victim_agg.values(), key=lambda x: x["count"], reverse=True)[:3]

        import tempfile
        tmp_path = tempfile.mktemp(suffix=".png")
        try:
            await _render_my_battle(
                user_qq=target_id,
                user_name=target_name,
                attacker_records=attacker_records,
                fancao_count=fancao_count,
                caoquanqun_count=caoquanqun_count,
                top_victims=top_victims,
                out_path=tmp_path,
                cache_dir=os.path.join(self.curr_dir, "avatar_cache"),
                titles_path=os.path.join(self.curr_dir, "qy_battle_titles.json"),
            )
            yield event.image_result(tmp_path)
        except Exception as e:
            logger.error(f"渲染我的战绩失败: {e}")
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    # ============================================================
    # 插件卸载清理
    # ============================================================

    async def terminate(self):
        save_json(self.cao_stats_file,       self.cao_stats)
        save_json(self.cao_records_file,     self.cao_records)
        save_json(self.cao_daily_file,       self.cao_daily)
        save_json(self.qy_body_file,         self.qy_body_data)
        save_json(self.qy_battle_file,       self.qy_battle_data)
        save_json(self.outside_stats_file,   self.outside_stats_data)
        save_json(self.cao_quan_qun_cd_file,  self.cao_quan_qun_cd)
        save_json(self.cao_quan_qun_log_file, self.cao_quan_qun_log)

        if self._daily_announce_task and not self._daily_announce_task.done():
            self._daily_announce_task.cancel()

        for task in tuple(self._withdraw_tasks):
            task.cancel()
        self._withdraw_tasks.clear()

    # ============================================================
    # /我的体内（单人档案）
    # ============================================================

    @filter.command(CMD_QY_PROFILE)
    async def qy_profile(self, event: AstrMessageEvent):
        async for result in self._cmd_qy_profile(event):
            yield result

    async def _cmd_qy_profile(self, event: AstrMessageEvent):
        if event.is_private_chat():
            yield event.plain_result("此功能仅在群聊中可用哦~")
            return

        group_id = str(event.get_group_id())
        if not is_allowed_group(group_id, self.config):
            return

        # 解析目标：@ 了就看别人，否则看自己
        sender_id = str(event.get_sender_id())
        target_id = sender_id
        for seg in event.get_messages():
            if hasattr(seg, "qq"):
                t = str(seg.qq)
                if t != sender_id:
                    target_id = t
                    break

        # 拉群成员信息
        user_map = {}
        try:
            if event.get_platform_name() == "aiocqhttp":
                assert isinstance(event, AiocqhttpMessageEvent)
                members = await event.bot.api.call_action(
                    "get_group_member_list", group_id=int(group_id)
                )
                if isinstance(members, dict) and "data" in members and isinstance(members["data"], list):
                    members = members["data"]
                for m in members:
                    uid = str(m.get("user_id"))
                    user_map[uid] = m.get("card") or m.get("nickname") or uid
        except Exception:
            pass

        target_name = user_map.get(target_id, f"用户({target_id})")

        # ── 月度体内数据 ─────────────────────────────────
        today = datetime.now()
        reset_date = today.strftime("%Y-%m-01")
        gdata = self.qy_body_data.get(group_id, {})
        if gdata.get("last_reset", "") != reset_date:
            gdata = {"total_ml": 0.0, "count": 0, "last_reset": reset_date, "users": {}}

        udata      = gdata.get("users", {}).get(target_id, {"ml": 0.0, "count": 0})
        month_ml   = udata.get("ml", 0.0)
        month_count= udata.get("count", 0)

        # 计算月内排名
        all_users = gdata.get("users", {})
        sorted_uids = sorted(all_users, key=lambda uid: all_users[uid].get("ml", 0.0), reverse=True)
        month_rank = 0
        for i, uid in enumerate(sorted_uids):
            if uid == target_id:
                month_rank = i + 1
                break
        month_total_users = len(sorted_uids)

        # 距下次刷新
        if today.month == 12:
            next_reset = datetime(today.year + 1, 1, 1)
        else:
            next_reset = datetime(today.year, today.month + 1, 1)
        delta      = next_reset - today
        days_left  = delta.days
        hours_left = delta.seconds // 3600

        # ── 30天被草历史记录 ─────────────────────────────
        self._clean_qy_battle()
        victim_records = (
            self.qy_battle_data
            .get(group_id, {})
            .get("victims", {})
            .get(target_id, {})
            .get("records", [])
        )

        # ── 来源分析（谁草了你最多） ──────────────────────
        attacker_agg: dict[str, dict] = {}
        for r in victim_records:
            aid = r.get("attacker_id", "")
            if not aid:
                continue
            if aid not in attacker_agg:
                attacker_agg[aid] = {
                    "uid":      aid,
                    "name":     user_map.get(aid, f"用户({aid})"),
                    "total_ml": 0.0,
                    "count":    0,
                }
            attacker_agg[aid]["total_ml"] = round(attacker_agg[aid]["total_ml"] + r["ml"], 1)
            attacker_agg[aid]["count"]   += 1

        import tempfile
        tmp_path = tempfile.mktemp(suffix=".png")
        try:
            await _render_qy_profile(
                target_qq=target_id,
                target_name=target_name,
                month_ml=month_ml,
                month_count=month_count,
                month_rank=month_rank,
                month_total_users=month_total_users,
                reset_date=reset_date,
                days_left=days_left,
                hours_left=hours_left,
                records_30d=victim_records,
                attacker_map=attacker_agg,
                out_path=tmp_path,
                cache_dir=os.path.join(self.curr_dir, "avatar_cache"),
                titles_path=os.path.join(self.curr_dir, "qy_body_titles.json"),
            )
            yield event.image_result(tmp_path)
        except Exception as e:
            logger.error(f"渲染体内档案失败: {e}")
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)