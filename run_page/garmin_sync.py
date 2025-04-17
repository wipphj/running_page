"""
Python 3 API wrapper for Garmin Connect to get your statistics.
Copy most code from https://github.com/cyberjunky/python-garminconnect
"""

import argparse
import asyncio
import logging
import os
import sys
import time
import traceback
import zipfile
from io import BytesIO

import aiofiles
import cloudscraper
import garth
import httpx
from config import FOLDER_DICT, JSON_FILE, SQL_FILE, config
from garmin_device_adaptor import wrap_device_info
from utils import make_activities_file

import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import io

# logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

TIME_OUT = httpx.Timeout(240.0, connect=360.0)
GARMIN_COM_URL_DICT = {
    "SSO_URL_ORIGIN": "https://sso.garmin.com",
    "SSO_URL": "https://sso.garmin.com/sso",
    "MODERN_URL": "https://connectapi.garmin.com",
    "SIGNIN_URL": "https://sso.garmin.com/sso/signin",
    "UPLOAD_URL": "https://connectapi.garmin.com/upload-service/upload/",
    "ACTIVITY_URL": "https://connectapi.garmin.com/activity-service/activity/{activity_id}",
}

GARMIN_CN_URL_DICT = {
    "SSO_URL_ORIGIN": "https://sso.garmin.com",
    "SSO_URL": "https://sso.garmin.cn/sso",
    "MODERN_URL": "https://connectapi.garmin.cn",
    "SIGNIN_URL": "https://sso.garmin.cn/sso/signin",
    "UPLOAD_URL": "https://connectapi.garmin.cn/upload-service/upload/",
    "ACTIVITY_URL": "https://connectapi.garmin.cn/activity-service/activity/{activity_id}",
}


class Garmin:
    def __init__(self, secret_string, auth_domain, is_only_running=False):
        """
        Init module
        """
        self.req = httpx.AsyncClient(timeout=TIME_OUT)
        self.cf_req = cloudscraper.CloudScraper()
        self.URL_DICT = (
            GARMIN_CN_URL_DICT
            if auth_domain and str(auth_domain).upper() == "CN"
            else GARMIN_COM_URL_DICT
        )
        if auth_domain and str(auth_domain).upper() == "CN":
            garth.configure(domain="garmin.cn")
        self.modern_url = self.URL_DICT.get("MODERN_URL")
        garth.client.loads(secret_string)
        if garth.client.oauth2_token.expired:
            garth.client.refresh_oauth2()

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36",
            "origin": self.URL_DICT.get("SSO_URL_ORIGIN"),
            "nk": "NT",
            "Authorization": str(garth.client.oauth2_token),
        }
        self.is_only_running = is_only_running
        self.upload_url = self.URL_DICT.get("UPLOAD_URL")
        self.activity_url = self.URL_DICT.get("ACTIVITY_URL")

    async def fetch_data(self, url, retrying=False):
        """
        Fetch and return data
        """
        try:
            response = await self.req.get(url, headers=self.headers)
            if response.status_code == 429:
                raise GarminConnectTooManyRequestsError("Too many requests")
            logger.debug(f"fetch_data got response code {response.status_code}")
            response.raise_for_status()
            return response.json()
        except Exception as err:
            print(err)
            if retrying:
                logger.debug(
                    "Exception occurred during data retrieval, relogin without effect: %s"
                    % err
                )
                raise GarminConnectConnectionError("Error connecting") from err
            else:
                logger.debug(
                    "Exception occurred during data retrieval - perhaps session expired - trying relogin: %s"
                    % err
                )
                await self.fetch_data(url, retrying=True)

    async def get_activities(self, start, limit):
        """
        Fetch available activities
        """
        url = f"{self.modern_url}/activitylist-service/activities/search/activities?start={start}&limit={limit}"
        if self.is_only_running:
            url = url + "&activityType=running"
        return await self.fetch_data(url)

    async def get_activity_summary(self, activity_id):
        """
        Fetch activity summary
        """
        url = f"{self.modern_url}/activity-service/activity/{activity_id}"
        return await self.fetch_data(url)

    async def download_activity(self, activity_id, file_type="gpx"):
        url = f"{self.modern_url}/download-service/export/{file_type}/activity/{activity_id}"
        if file_type == "fit":
            url = f"{self.modern_url}/download-service/files/activity/{activity_id}"
        logger.info(f"Download activity from {url}")
        response = await self.req.get(url, headers=self.headers)
        response.raise_for_status()
        return response.read()

    async def upload_activities_original_from_strava(
        self, datas, use_fake_garmin_device=False
    ):
        print(
            "start upload activities to garmin!, use_fake_garmin_device:",
            use_fake_garmin_device,
        )
        for data in datas:
            print(data.filename)
            with open(data.filename, "wb") as f:
                for chunk in data.content:
                    f.write(chunk)
            f = open(data.filename, "rb")
            # wrap fake garmin device to origin fit file, current not support gpx file
            if use_fake_garmin_device:
                file_body = wrap_device_info(f)
            else:
                file_body = BytesIO(f.read())
            files = {"file": (data.filename, file_body)}

            try:
                res = await self.req.post(
                    self.upload_url, files=files, headers=self.headers
                )
                os.remove(data.filename)
                f.close()
            except Exception as e:
                print(str(e))
                # just pass for now
                continue
            try:
                resp = res.json()["detailedImportResult"]
                print("garmin upload success: ", resp)
            except Exception as e:
                print("garmin upload failed: ", e)
        await self.req.aclose()

    async def upload_activity_from_file(self, file):
        print("Uploading " + str(file))
        f = open(file, "rb")

        file_body = BytesIO(f.read())
        files = {"file": (file, file_body)}

        try:
            res = await self.req.post(
                self.upload_url, files=files, headers=self.headers
            )
            f.close()
        except Exception as e:
            print(str(e))
            # just pass for now
            return
        try:
            resp = res.json()["detailedImportResult"]
            print("garmin upload success: ", resp)
        except Exception as e:
            print("garmin upload failed: ", e)

    async def upload_activities_files(self, files):
        print("start upload activities to garmin!")

        await gather_with_concurrency(
            10,
            [self.upload_activity_from_file(file=f) for f in files],
        )

        await self.req.aclose()


class GarminConnectHttpError(Exception):
    def __init__(self, status):
        super(GarminConnectHttpError, self).__init__(status)
        self.status = status


class GarminConnectConnectionError(Exception):
    """Raised when communication ended in error."""

    def __init__(self, status):
        """Initialize."""
        super(GarminConnectConnectionError, self).__init__(status)
        self.status = status


class GarminConnectTooManyRequestsError(Exception):
    """Raised when rate limit is exceeded."""

    def __init__(self, status):
        """Initialize."""
        super(GarminConnectTooManyRequestsError, self).__init__(status)
        self.status = status


class GarminConnectAuthenticationError(Exception):
    """Raised when login returns wrong result."""

    def __init__(self, status):
        """Initialize."""
        super(GarminConnectAuthenticationError, self).__init__(status)
        self.status = status


async def download_garmin_data(client, activity_id, file_type="gpx"):
    folder = FOLDER_DICT.get(file_type, "gpx")
    try:
        file_data = await client.download_activity(activity_id, file_type=file_type)
        file_data = process_gpx_data_conditionally(file_data)
        file_path = os.path.join(folder, f"{activity_id}.{file_type}")
        need_unzip = False
        if file_type == "fit":
            file_path = os.path.join(folder, f"{activity_id}.zip")
            need_unzip = True
        async with aiofiles.open(file_path, "wb") as fb:
            await fb.write(file_data)
        if need_unzip:
            zip_file = zipfile.ZipFile(file_path, "r")
            for file_info in zip_file.infolist():
                zip_file.extract(file_info, folder)
                if file_info.filename.endswith(".fit"):
                    os.rename(
                        os.path.join(folder, f"{activity_id}_ACTIVITY.fit"),
                        os.path.join(folder, f"{activity_id}.fit"),
                    )
                elif file_info.filename.endswith(".gpx"):
                    os.rename(
                        os.path.join(folder, f"{activity_id}_ACTIVITY.gpx"),
                        os.path.join(FOLDER_DICT["gpx"], f"{activity_id}.gpx"),
                    )
                else:
                    os.remove(os.path.join(folder, file_info.filename))
            os.remove(file_path)
    except Exception as e:
        print(f"Failed to download activity {activity_id}: {str(e)}")
        traceback.print_exc()


async def get_activity_id_list(client, start=0):
    activities = await client.get_activities(start, 100)
    if len(activities) > 0:
        ids = list(map(lambda a: str(a.get("activityId", "")), activities))
        print("Syncing Activity IDs")
        return ids + await get_activity_id_list(client, start + 100)
    else:
        return []


async def gather_with_concurrency(n, tasks):
    semaphore = asyncio.Semaphore(n)

    async def sem_task(task):
        async with semaphore:
            return await task

    return await asyncio.gather(*(sem_task(task) for task in tasks))


def get_downloaded_ids(folder):
    return [i.split(".")[0] for i in os.listdir(folder) if not i.startswith(".")]


async def download_new_activities(
    secret_string, auth_domain, downloaded_ids, is_only_running, folder, file_type
):
    client = Garmin(secret_string, auth_domain, is_only_running)
    # because I don't find a para for after time, so I use garmin-id as filename
    # to find new run to generage
    activity_ids = await get_activity_id_list(client)
    to_generate_garmin_ids = list(set(activity_ids) - set(downloaded_ids))
    print(f"{len(to_generate_garmin_ids)} new activities to be downloaded")

    to_generate_garmin_id2title = {}
    for id in to_generate_garmin_ids:
        try:
            activity_summary = await client.get_activity_summary(id)
            activity_title = activity_summary.get("activityName", "")
            to_generate_garmin_id2title[id] = activity_title
        except Exception as e:
            print(f"Failed to get activity summary {id}: {str(e)}")
            continue

    start_time = time.time()
    await gather_with_concurrency(
        10,
        [
            download_garmin_data(client, id, file_type=file_type)
            for id in to_generate_garmin_ids
        ],
    )
    print(f"Download finished. Elapsed {time.time()-start_time} seconds")

    await client.req.aclose()
    return to_generate_garmin_ids, to_generate_garmin_id2title

def process_gpx_data_conditionally(gpx_data_bytes: bytes) -> bytes:
    """
    有条件地处理 GPX 数据，仅修改满足条件的 <trkpt> 下 <time> 标签的文本内容，
    并精确保留原始文件的所有其他结构、命名空间、空白等。

    条件与之前相同：
    - 元数据年份 < 2020
    - 元数据时间与第一个轨迹点时间差 >= 2 小时

    参数:
        gpx_data_bytes: 原始 GPX 文件内容 (bytes)。

    返回:
        bytes: 修改后的 GPX 文件内容 (bytes)，或在不满足条件或出错时返回原始内容。
    """
    # 内部查找时使用的命名空间定义 (仅用于 find/findall)
    internal_gpx_prefix = 'gpx'
    gpx_namespace_uri = 'http://www.topografix.com/GPX/1/1'
    find_namespaces = {internal_gpx_prefix: gpx_namespace_uri}

    metadata_dt = None
    first_trkpt_dt = None
    perform_modification = False # 标志是否需要执行修改

    try:
        # --- 步骤 1: 使用 ElementTree 解析以进行条件检查和定位 ---
        xml_file = io.BytesIO(gpx_data_bytes)
        try:
            tree = ET.parse(xml_file)
        except ET.ParseError as e:
             try:
                gpx_content_str = gpx_data_bytes.decode('utf-8')
                xml_file = io.StringIO(gpx_content_str)
                tree = ET.parse(xml_file)
             except (UnicodeDecodeError, ET.ParseError) as e2:
                print(f"错误：无法解析 GPX XML: {e2}。将返回原始数据。")
                return gpx_data_bytes
        root = tree.getroot()

        # --- 条件检查 ---
        # 1a. 查找并解析元数据时间
        metadata_time_el = root.find(f'.//{internal_gpx_prefix}:metadata/{internal_gpx_prefix}:time', find_namespaces)
        if metadata_time_el is None or metadata_time_el.text is None:
            print("警告：未找到元数据时间。将返回原始数据。")
            return gpx_data_bytes
        try:
            metadata_dt = datetime.fromisoformat(metadata_time_el.text.replace('Z', '+00:00'))
        except ValueError as e:
            print(f"错误：解析元数据时间戳失败: {e}。将返回原始数据。")
            return gpx_data_bytes

        # 1b. 检查年份条件
        if metadata_dt.year >= 2020:
            print(f"信息：元数据年份 ({metadata_dt.year}) >= 2020。不执行处理。")
            return gpx_data_bytes # 直接返回

        # 1c. 查找并解析第一个轨迹点时间
        first_trkpt_time_el = root.find(f'.//{internal_gpx_prefix}:trk/{internal_gpx_prefix}:trkseg/{internal_gpx_prefix}:trkpt/{internal_gpx_prefix}:time', find_namespaces)
        if first_trkpt_time_el is None or first_trkpt_time_el.text is None:
            print("警告：未找到第一个轨迹点时间。将返回原始数据。")
            return gpx_data_bytes
        try:
            first_trkpt_dt = datetime.fromisoformat(first_trkpt_time_el.text.replace('Z', '+00:00'))
        except ValueError as e:
            print(f"错误：解析第一个轨迹点时间戳失败: {e}。将返回原始数据。")
            return gpx_data_bytes

        # 1d. 检查时间差条件
        time_difference = abs(metadata_dt - first_trkpt_dt)
        two_hours = timedelta(hours=2)
        if time_difference >= two_hours:
            print(f"信息：元数据年份 < 2020 且时间差 ({time_difference}) >= 2 小时。准备修改时间。")
            perform_modification = True # 设置标志，表示需要修改
        else:
            print(f"信息：时间差 ({time_difference}) < 2 小时。无需修改。")
            return gpx_data_bytes # 直接返回

    except Exception as e:
        # 捕获解析和条件检查阶段的任何意外错误
        print(f"错误：在条件检查阶段发生错误: {e}。将返回原始数据。")
        return gpx_data_bytes

    # --- 步骤 2: 如果需要修改，收集需要替换的时间字符串 ---
    if perform_modification:
        changes_to_make = [] # 存储 (旧字符串, 新字符串) 的列表
        eight_hours = timedelta(hours=8)

        try:
            # 再次查找所有轨迹点时间元素 (这次是为了获取文本并计算新值)
            all_trkpt_time_els = root.findall(f'.//{internal_gpx_prefix}:trk/{internal_gpx_prefix}:trkseg/{internal_gpx_prefix}:trkpt/{internal_gpx_prefix}:time', find_namespaces)

            for time_el in all_trkpt_time_els:
                if time_el.text:
                    original_time_str = time_el.text.strip() # 去除可能的空白
                    try:
                        current_dt = datetime.fromisoformat(original_time_str.replace('Z', '+00:00'))
                        adjusted_dt = current_dt - eight_hours
                        adjusted_time_str = adjusted_dt.isoformat(timespec='milliseconds').replace('+00:00', 'Z')

                        # 只有当新旧字符串不同时才记录更改
                        if original_time_str != adjusted_time_str:
                            changes_to_make.append((original_time_str, adjusted_time_str))
                            # 注意：这里我们记录的是字符串本身，用于后续替换

                    except ValueError:
                        print(f"警告：在计算新值时跳过无效的时间格式: {original_time_str}")
                        continue

        except Exception as e:
            print(f"错误：在收集时间更改信息时发生错误: {e}。将返回原始数据。")
            return gpx_data_bytes

        # --- 步骤 3: 在原始字节数据上执行字符串替换 ---
        if not changes_to_make:
            print("信息：满足修改条件，但没有找到需要更改的时间戳。")
            return gpx_data_bytes # 没有需要替换的，返回原始数据

        print(f"信息：将执行 {len(changes_to_make)} 次时间戳内容的替换...")
        modified_gpx_bytes = gpx_data_bytes # 从原始字节开始

        # 使用 set 来处理可能重复的时间戳，确保每种替换只进行一次定义
        # （bytes.replace 会替换所有匹配项，这正是我们想要的）
        unique_changes = set(changes_to_make)

        try:
            for old_str, new_str in unique_changes:
                old_bytes = old_str.encode('utf-8')
                new_bytes = new_str.encode('utf-8')
                # 在整个字节序列中替换所有出现的 old_bytes 为 new_bytes
                modified_gpx_bytes = modified_gpx_bytes.replace(old_bytes, new_bytes)

            print("信息：时间戳替换完成。")
            return modified_gpx_bytes

        except Exception as e:
            print(f"错误：在执行字节替换时发生错误: {e}。将返回原始数据。")
            return gpx_data_bytes # 如果替换出错，返回原始的更安全

    else:
        # 如果 perform_modification 为 False (因为条件不满足)
        # 之前的逻辑已经 return 了，这里理论上不会执行，但作为代码完整性保留
        return gpx_data_bytes

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "secret_string", nargs="?", help="secret_string fro get_garmin_secret.py"
    )
    parser.add_argument(
        "--is-cn",
        dest="is_cn",
        action="store_true",
        help="if garmin accout is cn",
    )
    parser.add_argument(
        "--only-run",
        dest="only_run",
        action="store_true",
        help="if is only for running",
    )
    parser.add_argument(
        "--tcx",
        dest="download_file_type",
        action="store_const",
        const="tcx",
        default="gpx",
        help="to download personal documents or ebook",
    )
    parser.add_argument(
        "--fit",
        dest="download_file_type",
        action="store_const",
        const="fit",
        default="gpx",
        help="to download personal documents or ebook",
    )
    options = parser.parse_args()
    secret_string = options.secret_string
    auth_domain = (
        "CN" if options.is_cn else config("sync", "garmin", "authentication_domain")
    )
    file_type = options.download_file_type
    is_only_running = options.only_run
    if secret_string is None:
        print("Missing argument nor valid configuration file")
        sys.exit(1)
    folder = FOLDER_DICT.get(file_type, "gpx")
    # make gpx or tcx dir
    if not os.path.exists(folder):
        os.mkdir(folder)
    downloaded_ids = get_downloaded_ids(folder)

    if file_type == "fit":
        gpx_folder = FOLDER_DICT["gpx"]
        if not os.path.exists(gpx_folder):
            os.mkdir(gpx_folder)
        downloaded_gpx_ids = get_downloaded_ids(gpx_folder)
        # merge downloaded_ids:list
        downloaded_ids = list(set(downloaded_ids + downloaded_gpx_ids))

    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(
        download_new_activities(
            secret_string,
            auth_domain,
            downloaded_ids,
            is_only_running,
            folder,
            file_type,
        )
    )
    loop.run_until_complete(future)
    new_ids, id2title = future.result()
    # fit may contain gpx(maybe upload by user)
    if file_type == "fit":
        make_activities_file(
            SQL_FILE,
            FOLDER_DICT["gpx"],
            JSON_FILE,
            file_suffix="gpx",
            activity_title_dict=id2title,
        )
    make_activities_file(
        SQL_FILE, folder, JSON_FILE, file_suffix=file_type, activity_title_dict=id2title
    )
