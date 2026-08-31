import asyncio
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import gzip
import os
import re
import shutil
import xml.etree.ElementTree as ET
from xml.dom import minidom

import aiohttp
from opencc import OpenCC
from tqdm.asyncio import tqdm_asyncio
from tqdm import tqdm


# ============================================================
# 基础配置
# ============================================================

TZ_UTC_PLUS_8 = timezone(timedelta(hours=8))

# 繁体 -> 简体
CC = OpenCC("t2s")


# ============================================================
# M3U 地址
# ============================================================

M3U_URL = (
    "https://raw.githubusercontent.com/"
    "meishero/testipvt/"
    "refs/heads/main/newenshanipvt.m3u"
)


# ============================================================
# EPG 预处理规则
# ============================================================

def _adjust_timezone(programme, from_offset, to_offset):
    """
    修改节目时间的时区。
    """
    for attr in ("start", "stop"):
        val = programme.get(attr, "")

        if from_offset in val:
            programme.set(
                attr,
                val.replace(from_offset, to_offset)
            )


def _make_tz_rule(channel_keyword, from_offset, to_offset):
    """
    创建时区调整规则。
    """

    def rule(channel_name, programme):

        if channel_keyword in channel_name:

            _adjust_timezone(
                programme,
                from_offset,
                to_offset
            )

    return rule


PREPROCESS_RULES = [

    (
        "kuke31/xmlgz",

        _make_tz_rule(
            "天映经典",
            "+0800",
            "+0700"
        )
    ),

]


def preprocess_epg(url, epg_content):

    """
    对特定 EPG 源进行预处理。
    """

    rules = [

        rule

        for keyword, rule in PREPROCESS_RULES

        if keyword in url

    ]

    if not rules:
        return epg_content


    try:

        root = ET.fromstring(
            epg_content
        )

    except ET.ParseError:

        return epg_content


    # channel_id -> 频道名称
    channel_names = {}


    for channel in root.findall("channel"):

        cid = channel.get(
            "id",
            ""
        )

        names = [

            x.text

            for x in channel.findall("display-name")

            if x.text

        ]

        channel_names[cid] = (
            " ".join(names)
            + " "
            + cid
        )


    # 修改节目时间
    for programme in root.findall("programme"):

        cid = programme.get(
            "channel",
            ""
        )

        name = channel_names.get(
            cid,
            cid
        )

        for rule in rules:

            rule(
                name,
                programme
            )


    return ET.tostring(
        root,
        encoding="unicode"
    )


# ============================================================
# 频道名称标准化
# ============================================================

def normalize_channel_name(name):

    """
    频道匹配标准化。

    主要用于判断两个频道是不是同一个频道。

    示例：

        中視新聞台
        中视新闻台

    ->

        中视新闻台


        CCTV1
        CCTV-1
        CCTV1高清
        CCTV-1 HD

    ->

        CCTV-1
    """

    if not name:
        return ""


    # --------------------------------------------------------
    # 繁体 -> 简体
    # --------------------------------------------------------

    name = CC.convert(
        name
    )


    # --------------------------------------------------------
    # 去除首尾空格
    # --------------------------------------------------------

    name = name.strip()


    # --------------------------------------------------------
    # 去除普通空格
    # --------------------------------------------------------

    name = name.replace(
        " ",
        ""
    )


    # --------------------------------------------------------
    # 去除-
    # --------------------------------------------------------

    name = name.replace(
        "-",
        ""
    )

    # --------------------------------------------------------
    # 去除常见后缀
    # --------------------------------------------------------

    suffix_list = [

        "高清",

        "超高清",

        "HDTV",

        "HD",

        "FHD",

        "UHD",

        "标清",
        
        "频道",
        
        "台",
        
        "MCP", 
        
        "亚洲", 
        
        "粤语", 
        
        "国语",
        
        "版",
        
        "高码",
        
        "50FPS",
        
        "HEVC",
        
        "SDR",
        
        "10m",
        
        "36m",
        
        "120m",
        
        "IPV4",
        
        "IPV6",
        
        "[IPV4]",
        
        "[IPV6]",
        
        "「IPV4」",
        
        "「IPV6」",

    ]


    changed = True

    while changed:

        changed = False

        for suffix in suffix_list:

            if name.upper().endswith(
                suffix.upper()
            ):

                name = name[
                    :-len(suffix)
                ].strip()

                changed = True

                break


    # --------------------------------------------------------
    # CCTV 标准化
    # --------------------------------------------------------

    name = re.sub(
        r"^CCTV[-_－—]?(\d+)$",
        r"CCTV-\1",
        name,
        flags=re.I
    )


    # --------------------------------------------------------
    # CCTV-5+
    # --------------------------------------------------------

    name = re.sub(
        r"^CCTV[-_－—]?(\d+)[＋+]",
        r"CCTV-\1+",
        name,
        flags=re.I
    )


    return name.strip()


# ============================================================
# 下载 EPG
# ============================================================

async def fetch_epg(url):

    connector = aiohttp.TCPConnector(
        limit=16,
        ssl=False
    )

    headers = {
        "User-Agent":
        "okHttp/Mod‑1.5.0.0"
    }


    try:

        timeout = aiohttp.ClientTimeout(
            total=120
        )

        async with aiohttp.ClientSession(
            connector=connector,
            headers=headers,
            timeout=timeout,
            trust_env=True
        ) as session:

            async with session.get(
                url
            ) as response:

                response.raise_for_status()


                # ------------------------------------------------
                # gzip
                # ------------------------------------------------

                if url.lower().endswith(
                    ".gz"
                ):

                    data = await response.read()

                    return gzip.decompress(
                        data
                    ).decode(
                        "utf-8",
                        errors="ignore"
                    )


                # ------------------------------------------------
                # XML
                # ------------------------------------------------

                return await response.text(
                    encoding="utf-8"
                )


    except Exception as e:

        print(
            f"{url} 下载失败: {e}"
        )

        return None


# ============================================================
# 解析单个 EPG
# ============================================================

def parse_epg(epg_content):

    try:

        root = ET.fromstring(
            epg_content
        )

    except ET.ParseError as e:

        print(
            "XML解析失败:",
            e
        )

        return {}, defaultdict(list)


    channels = {}

    programmes = defaultdict(list)


    # 当前北京时间日期
    today = datetime.now(
        TZ_UTC_PLUS_8
    ).date()


    # ========================================================
    # 第一阶段：解析频道
    # ========================================================

    for channel in root.findall(
        "channel"
    ):

        original_id = channel.get(
            "id",
            ""
        ).strip()


        if not original_id:
            continue


        # channel ID 简繁转换
        channel_id = CC.convert(
            original_id
        ).strip()


        display_names = []


        for name in channel.findall(
            "display-name"
        ):

            if not name.text:
                continue


            # 这里只做显示名称清洗
            cname = CC.convert(
                name.text.strip()
            )


            if not cname:
                continue


            lang = name.get(
                "lang",
                "zh"
            )


            if cname not in [
                x[0]
                for x in display_names
            ]:

                display_names.append(
                    [
                        cname,
                        lang
                    ]
                )


        # ----------------------------------------------------
        # 重要：
        #
        # display-name 和 channel-id 都保留。
        #
        # 后面会把：
        #
        # 中视新闻台
        # 537
        #
        # 拆成两个独立频道。
        # ----------------------------------------------------

        if (
            channel_id
            and
            channel_id not in [
                x[0]
                for x in display_names
            ]
        ):

            display_names.append(
                [
                    channel_id,
                    "zh"
                ]
            )


        channels[channel_id] = display_names


    # ========================================================
    # 第二阶段：解析节目
    # ========================================================

    valid_channels = set()


    for programme in root.findall(
        "programme"
    ):

        original_channel = programme.get(
            "channel",
            ""
        )


        channel_id = CC.convert(
            original_channel
        ).strip()


        if not channel_id:
            continue


        try:

            start_text = re.sub(
                r"\s+",
                "",
                programme.get(
                    "start",
                    ""
                )
            )

            stop_text = re.sub(
                r"\s+",
                "",
                programme.get(
                    "stop",
                    ""
                )
            )


            start = datetime.strptime(
                start_text,
                "%Y%m%d%H%M%S%z"
            )

            stop = datetime.strptime(
                stop_text,
                "%Y%m%d%H%M%S%z"
            )


        except Exception:

            continue


        # ----------------------------------------------------
        # 转北京时间
        # ----------------------------------------------------

        start = start.astimezone(
            TZ_UTC_PLUS_8
        )

        stop = stop.astimezone(
            TZ_UTC_PLUS_8
        )


        # ----------------------------------------------------
        # 只保留与今天有关的节目
        #
        # 今天开始之前的节目全部删除
        # 今天结束之后的节目全部删除
        # ----------------------------------------------------

        if (
            start.date() != today
            and
            stop.date() != today
        ):

            continue


        valid_channels.add(
            channel_id
        )


        prog = ET.Element(
            "programme",
            {
                "start":
                start.strftime(
                    "%Y%m%d%H%M%S %z"
                ),

                "stop":
                stop.strftime(
                    "%Y%m%d%H%M%S %z"
                )
            }
        )


        # ----------------------------------------------------
        # title
        # ----------------------------------------------------

        titles = programme.findall(
            "title"
        )


        if titles:

            for title in titles:

                text = (
                    title.text.strip()
                    if title.text
                    else
                    "精彩节目"
                )


                # 中文节目名统一简体
                text = CC.convert(
                    text
                )


                t = ET.SubElement(
                    prog,
                    "title"
                )

                t.text = text


                if title.get(
                    "lang"
                ):

                    t.set(
                        "lang",
                        title.get(
                            "lang"
                        )
                    )

        else:

            t = ET.SubElement(
                prog,
                "title"
            )

            t.text = "精彩节目"


        # ----------------------------------------------------
        # desc
        # ----------------------------------------------------

        for desc in programme.findall(
            "desc"
        ):

            if not desc.text:
                continue


            d = ET.SubElement(
                prog,
                "desc"
            )


            d.text = CC.convert(
                desc.text.strip()
            )


            if desc.get(
                "lang"
            ):

                d.set(
                    "lang",
                    desc.get(
                        "lang"
                    )
                )


        programmes[
            channel_id
        ].append(
            prog
        )


    # --------------------------------------------------------
    # 删除没有当天节目的频道
    # --------------------------------------------------------

    channels = {

        cid: names

        for cid, names in channels.items()

        if cid in valid_channels

    }


    programmes = {

        cid: progs

        for cid, progs in programmes.items()

        if cid in valid_channels

    }


    return channels, programmes


# ============================================================
# 创建频道候选名称
# ============================================================

def get_channel_candidates(
    channel_id,
    display_names
):

    """
    一个 EPG channel 可能存在：

        id = 537

        display-name = 中视新闻台
        display-name = 537

    返回：

        [
            "537",
            "中视新闻台"
        ]

    注意：

    数字 ID 和频道名称都作为独立频道候选。
    """

    result = []


    # --------------------------------------------------------
    # channel id
    # --------------------------------------------------------

    cid = CC.convert(
        channel_id
    ).strip()


    if cid:

        result.append(
            cid
        )


    # --------------------------------------------------------
    # display-name
    # --------------------------------------------------------

    for name, lang in display_names:

        if not name:
            continue


        name = CC.convert(
            name.strip()
        )


        if not name:
            continue


        if name not in result:

            result.append(
                name
            )


    return result


# ============================================================
# 将单个 EPG 频道拆成独立频道
# ============================================================

def expand_single_channel(
    channel_id,
    display_names,
    programmes
):

    """
    将：

        channel id="537"

        display-name="中视新闻台"
        display-name="537"

    拆成：

        中视新闻台
        537

    每个频道共享同一份节目表。
    """

    candidates = get_channel_candidates(
        channel_id,
        display_names
    )


    result = []


    for name in candidates:

        normalized = normalize_channel_name(
            name
        )


        if not normalized:
            continue


        result.append(
            {
                "id": name,
                "match_key": normalized,
                "programmes": programmes
            }
        )


    return result


# ============================================================
# 全源频道合并
# ============================================================

def merge_all_sources(
    source_data
):

    """
    核心合并逻辑。

    规则：

    1. 所有 EPG 源全部参与
    2. 繁体 -> 简体
    3. 去掉 高清 / HD 等后缀
    4. CCTV 名称统一
    5. 相同频道只保留节目数量最多的源
    6. 不同频道全部保留
    7. 一个频道拥有多个 display-name 时拆开
    """


    # --------------------------------------------------------
    # match_key -> 最终频道
    # --------------------------------------------------------

    merged = {}


    # --------------------------------------------------------
    # 最终频道顺序
    # --------------------------------------------------------

    channel_order = []


    # --------------------------------------------------------
    # 遍历所有 EPG 源
    # --------------------------------------------------------

    for source_index, (
        url,
        channels,
        programmes
    ) in enumerate(source_data):


        print(
            f"\n合并第 {source_index + 1} 个源:"
        )

        print(
            url
        )


        for channel_id, display_names in channels.items():

            current_programmes = programmes.get(
                channel_id,
                []
            )


            if not current_programmes:
                continue


            # ------------------------------------------------
            # 一个原始 channel 拆成多个独立频道
            # ------------------------------------------------

            expanded = expand_single_channel(
                channel_id,
                display_names,
                current_programmes
            )


            for item in expanded:

                output_id = item["id"]

                match_key = item["match_key"]

                progs = item["programmes"]


                if not match_key:
                    continue


                current_count = len(
                    progs
                )


                # =================================================
                # 第一次出现
                # =================================================

                if match_key not in merged:

                    merged[match_key] = {

                        "id":
                        output_id,

                        "display_names":
                        [
                            output_id
                        ],

                        "programmes":
                        progs,

                        "count":
                        current_count,

                        "source":
                        url

                    }


                    channel_order.append(
                        match_key
                    )


                    continue


                # =================================================
                # 同频道
                # =================================================

                existing = merged[
                    match_key
                ]


                old_count = existing[
                    "count"
                ]


                # -------------------------------------------------
                # 节目数更多
                #
                # 整个节目表替换
                # -------------------------------------------------

                if current_count > old_count:

                    existing[
                        "id"
                    ] = output_id


                    existing[
                        "programmes"
                    ] = progs


                    existing[
                        "count"
                    ] = current_count


                    existing[
                        "source"
                    ] = url


                # -------------------------------------------------
                # 相同频道的名称全部保留
                # -------------------------------------------------

                if output_id not in existing[
                    "display_names"
                ]:

                    existing[
                        "display_names"
                    ].append(
                        output_id
                    )


    # =========================================================
    # 转换成最终结构
    # =========================================================

    all_channel_ids = []

    all_channel_names = {}

    all_programmes = {}


    for match_key in channel_order:

        item = merged[
            match_key
        ]


        cid = item[
            "id"
        ]


        # 防止最终 ID 重复
        if cid in all_channel_ids:

            continue


        all_channel_ids.append(
            cid
        )


        # -----------------------------------------------------
        # 最终输出名称：
        #
        # 只使用当前最终 channel id
        #
        # 例如：
        #
        # 中視新聞台
        #
        # 最终：
        #
        # 中视新闻台
        # -----------------------------------------------------

        display_names = []


        normalized_output_id = CC.convert(
            cid
        ).strip()


        if normalized_output_id:

            display_names.append(
                [
                    normalized_output_id,
                    "zh"
                ]
            )


        all_channel_names[
            cid
        ] = display_names


        all_programmes[
            cid
        ] = item[
            "programmes"
        ]


    print(
        "\n全源合并完成:"
    )

    print(
        "频道数量:",
        len(all_channel_ids)
    )


    return (
        all_channel_ids,
        all_channel_names,
        all_programmes
    )


# ============================================================
# XML 输出
# ============================================================

def write_to_xml(
    channel_ids,
    channel_names,
    programmes,
    filename
):

    os.makedirs(
        "output",
        exist_ok=True
    )


    now = datetime.now(
        TZ_UTC_PLUS_8
    ).strftime(
        "%Y%m%d%H%M%S %z"
    )


    root = ET.Element(
        "tv",
        {
            "date":
            now
        }
    )


    for cid in channel_ids:

        # ----------------------------------------------------
        # channel
        # ----------------------------------------------------

        channel = ET.SubElement(
            root,
            "channel",
            {
                "id":
                CC.convert(
                    cid
                )
            }
        )


        # ----------------------------------------------------
        # display-name
        # ----------------------------------------------------

        names = channel_names.get(
            cid,
            []
        )


        for name, lang in names:

            display = ET.SubElement(
                channel,
                "display-name",
                {
                    "lang":
                    "zh"
                }
            )


            display.text = CC.convert(
                name
            )


        # ----------------------------------------------------
        # programme
        # ----------------------------------------------------

        for original_prog in programmes.get(
            cid,
            []
        ):

            # 创建新的 programme
            prog = ET.SubElement(
                root,
                "programme",
                {
                    "start":
                    original_prog.get(
                        "start",
                        ""
                    ),

                    "stop":
                    original_prog.get(
                        "stop",
                        ""
                    ),

                    "channel":
                    CC.convert(
                        cid
                    )
                }
            )


            # ------------------------------------------------
            # title / desc 等子节点
            # ------------------------------------------------

            for child in original_prog:

                new_child = ET.SubElement(
                    prog,
                    child.tag,
                    dict(
                        child.attrib
                    )
                )


                if child.text:

                    # 中文内容统一简体
                    new_child.text = CC.convert(
                        child.text
                    )

                else:

                    new_child.text = child.text


    # --------------------------------------------------------
    # XML 美化
    # --------------------------------------------------------

    xml = ET.tostring(
        root,
        encoding="utf-8"
    )


    pretty = minidom.parseString(
        xml
    ).toprettyxml(
        indent="\t"
    )


    with open(
        filename,
        "w",
        encoding="utf-8"
    ) as f:

        f.write(
            pretty
        )


# ============================================================
# gzip
# ============================================================

def compress_to_gz(
    src,
    dst
):

    with open(
        src,
        "rb"
    ) as f1:

        with gzip.open(
            dst,
            "wb"
        ) as f2:

            shutil.copyfileobj(
                f1,
                f2
            )


# ============================================================
# config.txt
# ============================================================

def get_urls():

    urls = []


    if not os.path.exists(
        "config.txt"
    ):

        print(
            "未找到 config.txt"
        )

        return urls


    with open(
        "config.txt",
        encoding="utf-8"
    ) as f:

        for line in f:

            line = line.strip()


            if (
                line
                and
                not line.startswith("#")
            ):

                urls.append(
                    line
                )


    return urls


# ============================================================
# 获取 M3U tvg-id
# ============================================================

async def fetch_m3u_tvg_ids(url):
    """
    获取 M3U 中的原始 tvg-id。

    返回：
        {
            标准化后的名称: M3U原始tvg-id
        }

    例如：
        CCTV-1 -> CCTV1
        中视新闻台 -> 中視新聞台
    """

    headers = {
        "User-Agent": "okhttp/Mod-1.5.0.0"
    }

    try:
        async with aiohttp.ClientSession(
            headers=headers
        ) as session:

            async with session.get(url) as resp:

                if resp.status != 200:
                    print(
                        f"M3U读取失败，HTTP状态码: {resp.status}"
                    )
                    return {}

                text = await resp.text()

        # 提取原始 tvg-id
        ids = re.findall(
            r'^#EXTINF:.*?tvg-id=["\']([^"\']+)["\']',
            text,
            flags=re.M          # ← 补回来
        )

        result = {}

        for raw_id in ids:

            raw_id = raw_id.strip()

            if not raw_id:
                continue

            # 用标准化后的名称进行匹配
            normalized_id = normalize_channel_name(
                raw_id
            )

            if not normalized_id:
                continue

            # 保存：
            #
            # CCTV-1 -> CCTV1
            #
            # 也就是说：
            # key   = 用于匹配
            # value = 最终输出
            #
            if normalized_id not in result:
                result[normalized_id] = raw_id

        print(
            f"M3U原始频道数量: {len(ids)}"
        )

        print(
            f"M3U标准化匹配频道数量: {len(result)}"
        )

        return result

    except Exception as e:

        print(
            "M3U读取失败:",
            e
        )

        return {}

# ============================================================
# 根据 M3U 清洗
# ============================================================

def filter_by_m3u(
        channel_ids,
        channel_names,
        programmes,
        tvg_ids
):
    """
    根据 M3U tvg-id 清洗 all.xml。

    匹配：
        使用 normalize_channel_name()

    输出：
        使用 M3U 中的原始 tvg-id。

    例如：

        M3U:
            tvg-id="CCTV1"

        all.xml:
            channel id="CCTV-1"

        匹配时：
            CCTV1 -> CCTV-1
            CCTV-1 -> CCTV-1

        最终：
            channel id="CCTV1"
            display-name="CCTV1"
    """

    result_ids = []
    result_names = {}
    result_programmes = {}

    # 防止同一个 M3U tvg-id 被重复加入
    used_tvg_ids = set()

    for cid in channel_ids:

        # ====================================================
        # 1. 收集 all.xml 当前频道所有可能的匹配名称
        # ====================================================

        check_names = set()

        normalized_cid = normalize_channel_name(cid)

        if normalized_cid:
            check_names.add(normalized_cid)

        for name, _ in channel_names.get(cid, []):

            normalized_name = normalize_channel_name(
                name
            )

            if normalized_name:
                check_names.add(normalized_name)

        # ====================================================
        # 2. 和 M3U tvg-id 做标准化匹配
        # ====================================================

        matched_tvg_id = None

        for normalized_name in check_names:

            if normalized_name in tvg_ids:

                matched_tvg_id = tvg_ids[
                    normalized_name
                ]

                break

        # ====================================================
        # 3. 没匹配到
        # ====================================================

        if not matched_tvg_id:
            continue

        # 防止：
        #
        # CCTV-1
        # CCTV1
        #
        # 因为 all.xml 内存在多个等价频道，
        # 导致最终重复输出。
        if matched_tvg_id in used_tvg_ids:
            continue

        used_tvg_ids.add(
            matched_tvg_id
        )

        # ====================================================
        # 4. 最终 channel-id 使用 M3U 原始 tvg-id
        # ====================================================

        final_id = matched_tvg_id

        result_ids.append(
            final_id
        )

        # ====================================================
        # 5. 最终 display-name 也使用 M3U 原始 tvg-id
        # ====================================================

        result_names[final_id] = [
            [final_id, "zh"]
        ]

        # ====================================================
        # 6. 深拷贝节目数据
        #
        # 注意不能直接修改 all_programmes 中的 programme，
        # 否则会影响 all.xml 数据。
        # ====================================================

        result_programmes[final_id] = []

        for prog in programmes.get(cid, []):

            new_prog = ET.Element(
                "programme",
                attrib=dict(prog.attrib)
            )

            # 修改 channel
            new_prog.set(
                "channel",
                final_id
            )

            # 复制 title / desc
            for child in prog:

                new_child = ET.SubElement(
                    new_prog,
                    child.tag,
                    attrib=dict(child.attrib)
                )

                new_child.text = child.text

            result_programmes[final_id].append(
                new_prog
            )

    print(
        f"M3U清洗完成：最终保留 {len(result_ids)} 个频道"
    )

    return (
        result_ids,
        result_names,
        result_programmes
    )


# ============================================================
# 主程序
# ============================================================

async def main():

    # ========================================================
    # 读取 EPG 源
    # ========================================================

    urls = get_urls()


    if not urls:

        print(
            "没有可用 EPG 源"
        )

        return


    print(
        "========================================"
    )

    print(
        "EPG 合并开始"
    )

    print(
        "EPG 源数量:",
        len(urls)
    )

    print(
        "========================================"
    )


    # ========================================================
    # 第一阶段
    #
    # 并行下载全部 EPG
    # ========================================================

    tasks = [

        fetch_epg(
            url
        )

        for url in urls

    ]


    contents = await tqdm_asyncio.gather(
        *tasks,
        desc="Downloading EPG"
    )


    # ========================================================
    # 第二阶段
    #
    # 解析所有 EPG
    #
    # 注意：
    #
    # 这里先全部解析。
    #
    # 后面才统一进行频道合并。
    # ========================================================

    source_data = []


    for index, content in enumerate(
        contents
    ):

        if not content:

            print(
                f"\nEPG源失败，跳过:"
            )

            print(
                urls[index]
            )

            continue


        print(
            f"\n解析 EPG {index + 1}/{len(urls)}:"
        )

        print(
            urls[index]
        )


        # ----------------------------------------------------
        # 源预处理
        # ----------------------------------------------------

        content = preprocess_epg(
            urls[index],
            content
        )


        # ----------------------------------------------------
        # 解析
        # ----------------------------------------------------

        channels, programmes = parse_epg(
            content
        )


        print(
            "有效频道:",
            len(channels)
        )


        source_data.append(
            (
                urls[index],
                channels,
                programmes
            )
        )


    # ========================================================
    # 第三阶段
    #
    # 全源合并
    #
    # 同频道：
    #       节目数最多的源胜出
    #
    # 不同频道：
    #       全部保留
    #
    # 多 display-name：
    #       拆成独立频道
    # ========================================================

    (
        all_ids,
        all_names,
        all_programmes
    ) = merge_all_sources(
        source_data
    )


    # ========================================================
    # 第四阶段
    #
    # 输出 all.xml
    # ========================================================

    print(
        "\n生成 output/all.xml"
    )


    write_to_xml(
        all_ids,
        all_names,
        all_programmes,
        "output/all.xml"
    )


    # ========================================================
    # 第五阶段
    #
    # 压缩 all.gz
    # ========================================================

    print(
        "生成 output/all.gz"
    )


    compress_to_gz(
        "output/all.xml",
        "output/all.gz"
    )


    # ========================================================
    # 第六阶段
    #
    # 下载 M3U
    # ========================================================

    print(
        "\n开始读取 M3U:"
    )

    print(
        M3U_URL
    )


    tvg_ids = await fetch_m3u_tvg_ids(
        M3U_URL
    )


    # ========================================================
    # 第七阶段
    #
    # M3U 清洗
    # ========================================================

    if tvg_ids:

        (
            filtered_ids,
            filtered_names,
            filtered_programmes
        ) = filter_by_m3u(

            all_ids,

            all_names,

            all_programmes,

            tvg_ids

        )

    else:

        print(
            "M3U 获取失败，不进行清洗"
        )


        filtered_ids = all_ids

        filtered_names = all_names

        filtered_programmes = all_programmes


    # ========================================================
    # 第八阶段
    #
    # 输出最终 epg.xml
    # ========================================================

    print(
        "\n生成 output/epg.xml"
    )


    write_to_xml(
        filtered_ids,
        filtered_names,
        filtered_programmes,
        "output/epg.xml"
    )


    # ========================================================
    # 第九阶段
    #
    # 压缩 epg.gz
    # ========================================================

    print(
        "生成 output/epg.gz"
    )


    compress_to_gz(
        "output/epg.xml",
        "output/epg.gz"
    )


    # ========================================================
    # 完成
    # ========================================================

    print(
        "\n========================================"
    )

    print(
        "EPG 更新完成"
    )

    print(
        "all.xml 频道:",
        len(all_ids)
    )

    print(
        "epg.xml 频道:",
        len(filtered_ids)
    )

    print(
        "========================================"
    )


# ============================================================
# 程序入口
# ============================================================

if __name__ == "__main__":

    asyncio.run(
        main()
    )
