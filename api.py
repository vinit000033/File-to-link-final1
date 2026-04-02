import os
import asyncio
import hashlib
import math
import socket
import urllib.parse
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from math import ceil, floor
from mimetypes import guess_type
from datetime import datetime
from typing import Optional, List, AsyncGenerator, Union, Awaitable, DefaultDict, Tuple, BinaryIO

from fastapi import FastAPI, HTTPException, Response, Request
from fastapi.responses import StreamingResponse, HTMLResponse
from fastapi.templating import Jinja2Templates

from telethon import utils, helpers, TelegramClient
from telethon.crypto import AuthKey
from telethon.errors import FloodWaitError
from telethon.network import MTProtoSender
from telethon.sessions import SQLiteSession
from telethon.tl.alltlobjects import LAYER
from telethon.tl.custom import Message
from telethon.tl.functions import InvokeWithLayerRequest
from telethon.tl.functions.auth import ExportAuthorizationRequest, ImportAuthorizationRequest
from telethon.tl.functions.upload import GetFileRequest, SaveFilePartRequest, SaveBigFilePartRequest
from telethon.tl.types import (
    Document,
    InputFileLocation,
    InputDocumentFileLocation,
    InputPhotoFileLocation,
    InputPeerPhotoFileLocation,
    TypeInputFile,
    InputFileBig,
    InputFile,
)

try:
    from config import API_ID, API_HASH, BOT_TOKEN, LOG_CHANNEL_ID
    from utils import LOGGER
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO)
    LOGGER = logging.getLogger(__name__)
    API_ID = os.getenv("API_ID")
    API_HASH = os.getenv("API_HASH")
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    LOG_CHANNEL_ID = int(os.getenv("LOG_CHANNEL_ID", "0"))

HEROKU_APP_NAME = os.getenv("HEROKU_APP_NAME")
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL")
RAILWAY_PUBLIC_DOMAIN = os.getenv("RAILWAY_PUBLIC_DOMAIN")
RAILWAY_STATIC_URL = os.getenv("RAILWAY_STATIC_URL")
FLY_APP_NAME = os.getenv("FLY_APP_NAME")
VERCEL_URL = os.getenv("https://filetolink-ten.vercel.app")
CUSTOM_DOMAIN = os.getenv("CUSTOM_DOMAIN")

TypeLocation = Union[
    Document,
    InputDocumentFileLocation,
    InputPeerPhotoFileLocation,
    InputFileLocation,
    InputPhotoFileLocation,
]


class Telegram:
    API_ID = API_ID
    API_HASH = API_HASH
    BOT_TOKEN = BOT_TOKEN
    CHANNEL_ID = LOG_CHANNEL_ID


class Server:
    BIND_ADDRESS = "0.0.0.0"
    PORT = int(os.getenv("PORT", 5000))
    BASE_URL = None


templates = Jinja2Templates(directory="templates")

error_messages = {
    400: "Invalid request.",
    401: "File code is required to download the file.",
    403: "Invalid file code.",
    404: "File not found.",
    416: "Invalid range.",
    500: "Internal server error.",
    503: "Service temporarily unavailable.",
}


class DownloadSender:
    def __init__(
        self,
        client: TelegramClient,
        sender: MTProtoSender,
        file: TypeLocation,
        offset: int,
        limit: int,
        stride: int,
        count: int,
    ) -> None:
        self.sender = sender
        self.client = client
        self.request = GetFileRequest(file, offset=offset, limit=limit)
        self.stride = stride
        self.remaining = count

    async def next(self) -> Optional[bytes]:
        if not self.remaining:
            return None
        result = await self.client._call(self.sender, self.request)
        self.remaining -= 1
        self.request.offset += self.stride
        return result.bytes

    def disconnect(self) -> Awaitable[None]:
        return self.sender.disconnect()


class ParallelTransferrer:
    def __init__(self, client: TelegramClient, dc_id: Optional[int] = None) -> None:
        self.client = client
        self.loop = self.client.loop
        self.dc_id = dc_id or self.client.session.dc_id
        self.auth_key = (
            None
            if dc_id and self.client.session.dc_id != dc_id
            else self.client.session.auth_key
        )
        self.senders: Optional[List[DownloadSender]] = None

    async def _cleanup(self) -> None:
        await asyncio.gather(*[sender.disconnect() for sender in self.senders])
        self.senders = None

    @staticmethod
    def _get_connection_count(
        file_size: int,
        max_count: int = 20,
        full_size: int = 100 * 1024 * 1024,
    ) -> int:
        if file_size > full_size:
            return max_count
        return math.ceil((file_size / full_size) * max_count)

    async def _create_sender(self) -> MTProtoSender:
        dc = await self.client._get_dc(self.dc_id)
        sender = MTProtoSender(self.auth_key, loggers=self.client._log)
        await sender.connect(
            self.client._connection(
                dc.ip_address,
                dc.port,
                dc.id,
                loggers=self.client._log,
                proxy=self.client._proxy,
            )
        )
        if not self.auth_key:
            LOGGER.debug("Exporting auth to DC %s", self.dc_id)
            auth = await self.client(ExportAuthorizationRequest(self.dc_id))
            self.client._init_request.query = ImportAuthorizationRequest(
                id=auth.id, bytes=auth.bytes
            )
            req = InvokeWithLayerRequest(LAYER, self.client._init_request)
            await sender.send(req)
            self.auth_key = sender.auth_key
        return sender

    async def _create_download_sender(
        self,
        file: TypeLocation,
        index: int,
        part_size: int,
        stride: int,
        part_count: int,
    ) -> DownloadSender:
        return DownloadSender(
            self.client,
            await self._create_sender(),
            file,
            index * part_size,
            part_size,
            stride,
            part_count,
        )

    async def _init_download(
        self,
        connections: int,
        file: TypeLocation,
        part_count: int,
        part_size: int,
    ) -> None:
        minimum, remainder = divmod(part_count, connections)

        def get_part_count() -> int:
            nonlocal remainder
            if remainder > 0:
                remainder -= 1
                return minimum + 1
            return minimum

        self.senders = [
            await self._create_download_sender(
                file, 0, part_size, connections * part_size, get_part_count()
            ),
            *await asyncio.gather(
                *[
                    self._create_download_sender(
                        file, i, part_size, connections * part_size, get_part_count()
                    )
                    for i in range(1, connections)
                ]
            ),
        ]

    async def download(
        self,
        file: TypeLocation,
        file_size: int,
        part_size_kb: Optional[float] = None,
        connection_count: Optional[int] = None,
    ) -> AsyncGenerator[bytes, None]:
        connection_count = connection_count or self._get_connection_count(file_size)
        part_size = int(
            (part_size_kb or utils.get_appropriated_part_size(file_size)) * 1024
        )
        part_count = math.ceil(file_size / part_size)
        LOGGER.debug(
            "Parallel download: connections=%s part_size=%s part_count=%s",
            connection_count,
            part_size,
            part_count,
        )
        await self._init_download(connection_count, file, part_count, part_size)
        part = 0
        while part < part_count:
            tasks = [
                self.loop.create_task(sender.next()) for sender in self.senders
            ]
            for task in tasks:
                data = await task
                if not data:
                    break
                yield data
                part += 1
        LOGGER.debug("Parallel download finished, cleaning up connections")
        await self._cleanup()


parallel_transfer_locks: DefaultDict[int, asyncio.Lock] = defaultdict(
    lambda: asyncio.Lock()
)


def get_base_url_from_request(request: Request) -> str:
    forwarded_proto = request.headers.get("x-forwarded-proto")
    forwarded_host = request.headers.get("x-forwarded-host")
    host = request.headers.get("host")
    if forwarded_host:
        scheme = forwarded_proto or "https"
        return f"{scheme}://{forwarded_host}"
    elif host:
        scheme = "https" if forwarded_proto == "https" else "http"
        return f"{scheme}://{host}"
    else:
        return Server.BASE_URL or f"http://localhost:{Server.PORT}"


def abort(status_code: int = 500, description: str = None):
    raise HTTPException(
        status_code=status_code,
        detail=description or error_messages.get(status_code),
    )


def sanitize_filename(filename: str) -> str:
    try:
        filename.encode("latin-1")
        return filename
    except UnicodeEncodeError:
        return urllib.parse.quote(filename, safe="")


def get_file_properties(message: Message):
    file_name = message.file.name
    file_size = message.file.size or 0
    mime_type = message.file.mime_type
    if not file_name:
        attributes = {
            "video": "mp4",
            "audio": "mp3",
            "voice": "ogg",
            "photo": "jpg",
            "video_note": "mp4",
        }
        for attribute, extension in attributes.items():
            media = getattr(message, attribute, None)
            if media:
                file_type = attribute
                file_format = extension
                break
        else:
            abort(400, "Invalid media type.")
        date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        file_name = f"{file_type}-{date}.{file_format}"
    if not mime_type:
        mime_type = guess_type(file_name)[0] or "application/octet-stream"
    return file_name, file_size, mime_type


class FileToLinkAPI(TelegramClient):
    def __init__(self, api_id, api_hash, bot_token):
        LOGGER.info(
            "Creating Telethon FileToLink Client with SQLiteSession at /tmp/telethon.session"
        )
        super().__init__(
            SQLiteSession("/tmp/telethon.session"),
            api_id,
            api_hash,
            connection_retries=-1,
            timeout=120,
            flood_sleep_threshold=0,
            request_retries=10,
            auto_reconnect=True,
        )
        self.bot_token = bot_token
        self.max_concurrent = 500
        self.semaphore = asyncio.Semaphore(self.max_concurrent)

    async def start_api(self):
        while True:
            try:
                await self.start(bot_token=self.bot_token)
                LOGGER.info("Telethon FileToLink Client started successfully!")
                LOGGER.info(
                    "Max concurrent requests: %s", self.max_concurrent
                )
                return
            except FloodWaitError as e:
                LOGGER.warning(
                    "FloodWait during startup: waiting %d seconds", e.seconds
                )
                await asyncio.sleep(e.seconds)

    async def fast_iter_download(
        self,
        message: Message,
        offset: int = 0,
        limit: int = None,
    ) -> AsyncGenerator[bytes, None]:
        file_size = message.file.size
        if limit is None:
            limit = file_size

        dc_id, location = utils.get_input_location(message.media)
        async with parallel_transfer_locks[dc_id]:
            transferrer = ParallelTransferrer(self, dc_id)
            connection_count = ParallelTransferrer._get_connection_count(file_size)
            part_size = int(
                utils.get_appropriated_part_size(file_size) * 1024
            )
            part_count = math.ceil(file_size / part_size)

            LOGGER.debug(
                "FastIterDownload: dc=%s connections=%s parts=%s part_size=%s",
                dc_id,
                connection_count,
                part_count,
                part_size,
            )

            await transferrer._init_download(
                connection_count, location, part_count, part_size
            )

            bytes_yielded = 0
            part = 0
            while part < part_count and bytes_yielded < limit:
                tasks = [
                    self.loop.create_task(sender.next())
                    for sender in transferrer.senders
                ]
                for task in tasks:
                    data = await task
                    if not data:
                        break
                    if bytes_yielded + len(data) > limit:
                        yield data[: limit - bytes_yielded]
                        bytes_yielded = limit
                        break
                    yield data
                    bytes_yielded += len(data)
                    part += 1
                    if bytes_yielded >= limit:
                        break

            await transferrer._cleanup()


async def get_local_ip() -> str:
    loop = asyncio.get_event_loop()
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setblocking(False)
    try:
        await loop.sock_connect(s, ("8.8.8.8", 80))
        ip = s.getsockname()[0]
    except Exception:
        ip = "127.0.0.1"
    finally:
        s.close()
    return ip


async def detect_base_url() -> str:
    if CUSTOM_DOMAIN:
        base_url = (
            f"https://{CUSTOM_DOMAIN}"
            if not CUSTOM_DOMAIN.startswith("http")
            else CUSTOM_DOMAIN
        )
        LOGGER.info("Using CUSTOM_DOMAIN: %s", base_url)
        return base_url
    if HEROKU_APP_NAME:
        base_url = f"https://{HEROKU_APP_NAME}.herokuapp.com"
        LOGGER.info("Detected Heroku: %s", base_url)
        return base_url
    if RENDER_EXTERNAL_URL:
        base_url = RENDER_EXTERNAL_URL.rstrip("/")
        LOGGER.info("Detected Render: %s", base_url)
        return base_url
    if RAILWAY_PUBLIC_DOMAIN:
        base_url = f"https://{RAILWAY_PUBLIC_DOMAIN}"
        LOGGER.info("Detected Railway (public domain): %s", base_url)
        return base_url
    if RAILWAY_STATIC_URL:
        base_url = RAILWAY_STATIC_URL.rstrip("/")
        LOGGER.info("Detected Railway (static URL): %s", base_url)
        return base_url
    if FLY_APP_NAME:
        base_url = f"https://{FLY_APP_NAME}.fly.dev"
        LOGGER.info("Detected Fly.io: %s", base_url)
        return base_url
    if VERCEL_URL:
        base_url = f"https://{VERCEL_URL}"
        LOGGER.info("Detected Vercel: %s", base_url)
        return base_url
    ip = await get_local_ip()
    base_url = f"http://{ip}:{Server.PORT}"
    LOGGER.info("No platform detected, using local IP: %s", base_url)
    return base_url


@asynccontextmanager
async def lifespan(app: FastAPI):
    Server.BASE_URL = await detect_base_url()
    await api_instance.start_api()
    LOGGER.info("API running on: %s", Server.BASE_URL)
    yield
    LOGGER.info("Shutting down API")
    await api_instance.disconnect()


app = FastAPI(lifespan=lifespan, title="FileToLink")


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    try:
        return templates.TemplateResponse(request=request, name="index.html")
    except Exception as e:
        return HTMLResponse(
            content=f"<h1>FileToLink API</h1><p>Status: Running</p><p>Error loading template: {str(e)}</p>"
        )


@app.get("/stream/{file_id}", response_class=HTMLResponse)
async def stream_file(file_id: int, request: Request):
    code = request.query_params.get("code") or abort(401)
    async with api_instance.semaphore:
        me = await api_instance.get_me()
        api_name = "@" + me.username
        LOGGER.info("Stream page request - File ID: %s API: %s", file_id, api_name)

        try:
            file = await asyncio.wait_for(
                api_instance.get_messages(Telegram.CHANNEL_ID, ids=int(file_id)),
                timeout=15.0,
            )
        except asyncio.TimeoutError:
            LOGGER.error("Timeout retrieving message %s", file_id)
            abort(500, "Request timeout")
        except Exception as e:
            LOGGER.error("Failed to retrieve message %s: %s", file_id, e)
            abort(500)

        if not file:
            LOGGER.warning("Message %s not found in channel %s", file_id, Telegram.CHANNEL_ID)
            abort(404)

        if code != file.raw_text:
            LOGGER.warning(
                "Access denied for file %s: provided=%s expected=%s",
                file_id, code, file.raw_text,
            )
            abort(403)

        file_name, file_size, mime_type = get_file_properties(file)
        LOGGER.info("File: %s Size: %s Type: %s", file_name, file_size, mime_type)

        quoted_code = urllib.parse.quote(code)
        base_url = get_base_url_from_request(request)
        file_url = f"{base_url}/dl/{file_id}?code={quoted_code}"
        file_size_mb = f"{file_size / (1024 * 1024):.2f} MB"

        try:
            return templates.TemplateResponse(
                request=request,
                name="player.html",
                context={
                    "file_name": file_name,
                    "file_size_mb": file_size_mb,
                    "file_url": file_url,
                    "mime_type": mime_type,
                },
            )
        except Exception as e:
            return HTMLResponse(
                content=f"<h1>{file_name}</h1><p>Size: {file_size_mb}</p><a href='{file_url}'>Download</a>"
            )


async def _resolve_file_and_validate(file_id: int, code: str):
    try:
        file = await asyncio.wait_for(
            api_instance.get_messages(Telegram.CHANNEL_ID, ids=int(file_id)),
            timeout=15.0,
        )
    except asyncio.TimeoutError:
        LOGGER.error("Timeout retrieving message %s", file_id)
        abort(500, "Request timeout")
    except Exception as e:
        LOGGER.error("Failed to retrieve message %s: %s", file_id, e)
        abort(500)

    if not file:
        LOGGER.warning("Message %s not found in channel %s", file_id, Telegram.CHANNEL_ID)
        abort(404)

    if code != file.raw_text:
        LOGGER.warning(
            "Access denied for file %s: provided=%s expected=%s",
            file_id, code, file.raw_text,
        )
        abort(403)

    return file


@app.get("/dl/{file_id}")
async def transmit_file(file_id: int, request: Request):
    code = request.query_params.get("code") or abort(401)

    if code.endswith("=stream"):
        code_clean = code[:-7]
        quoted_code = urllib.parse.quote(code_clean)
        base_url = get_base_url_from_request(request)

        async with api_instance.semaphore:
            me = await api_instance.get_me()
            LOGGER.info("Stream redirect from dl - File ID: %s API: @%s", file_id, me.username)
            file = await _resolve_file_and_validate(file_id, code_clean)
            file_name, file_size, mime_type = get_file_properties(file)
            file_url = f"{base_url}/dl/{file_id}?code={quoted_code}"
            file_size_mb = f"{file_size / (1024 * 1024):.2f} MB"
            try:
                return templates.TemplateResponse(
                    request=request,
                    name="player.html",
                    context={
                        "file_name": file_name,
                        "file_size_mb": file_size_mb,
                        "file_url": file_url,
                        "mime_type": mime_type,
                    },
                )
            except Exception as e:
                return HTMLResponse(
                    content=f"<h1>{file_name}</h1><p>Size: {file_size_mb}</p><a href='{file_url}'>Download</a>"
                )

    async with api_instance.semaphore:
        me = await api_instance.get_me()
        LOGGER.info("Download request - File ID: %s API: @%s", file_id, me.username)

        file = await _resolve_file_and_validate(file_id, code)
        file_name, file_size, mime_type = get_file_properties(file)
        LOGGER.info("File: %s Size: %s Type: %s", file_name, file_size, mime_type)

        range_header = request.headers.get("Range", "")
        if range_header:
            range_str = range_header.replace("bytes=", "")
            parts = range_str.split("-")
            from_bytes = int(parts[0])
            until_bytes = int(parts[1]) if parts[1] else file_size - 1
            LOGGER.info("Range request: %s-%s/%s", from_bytes, until_bytes, file_size)
        else:
            from_bytes = 0
            until_bytes = file_size - 1
            LOGGER.info("Full file request: %s bytes", file_size)

        if (until_bytes > file_size) or (from_bytes < 0) or (until_bytes < from_bytes):
            LOGGER.error("Invalid range: %s-%s/%s", from_bytes, until_bytes, file_size)
            abort(416, "Invalid range.")

        chunk_size = 1 * 1024 * 1024
        until_bytes = min(until_bytes, file_size - 1)
        offset = from_bytes - (from_bytes % chunk_size)
        first_part_cut = from_bytes - offset
        last_part_cut = until_bytes % chunk_size + 1
        req_length = until_bytes - from_bytes + 1
        part_count = ceil(until_bytes / chunk_size) - floor(offset / chunk_size)

        sanitized_filename = sanitize_filename(file_name)

        headers = {
            "Content-Type": mime_type,
            "Content-Range": f"bytes {from_bytes}-{until_bytes}/{file_size}",
            "Content-Length": str(req_length),
            "Content-Disposition": f"attachment; filename*=UTF-8''{sanitized_filename}",
            "Accept-Ranges": "bytes",
            "Cache-Control": "public, max-age=3600",
            "Connection": "keep-alive",
        }

        LOGGER.info(
            "Starting parallel download: API=@%s chunks=%s chunk_size=%s",
            me.username, part_count, chunk_size,
        )

        async def file_generator():
            dc_id, location = utils.get_input_location(file.media)
            transferrer = ParallelTransferrer(api_instance, dc_id)
            connection_count = max(
                1, ParallelTransferrer._get_connection_count(file_size)
            )
            part_size = chunk_size
            total_parts = math.ceil(file_size / part_size)

            LOGGER.debug(
                "ParallelTransferrer: dc=%s connections=%s total_parts=%s",
                dc_id, connection_count, total_parts,
            )

            await transferrer._init_download(
                connection_count, location, total_parts, part_size
            )

            current_part = 0
            bytes_yielded_total = 0

            try:
                while current_part < total_parts:
                    batch_tasks = [
                        asyncio.ensure_future(sender.next())
                        for sender in transferrer.senders
                    ]
                    for task in batch_tasks:
                        try:
                            chunk = await asyncio.wait_for(task, timeout=30.0)
                        except asyncio.TimeoutError:
                            LOGGER.warning("Chunk timeout at part %s, skipping", current_part)
                            current_part += 1
                            continue

                        if chunk is None:
                            break

                        if current_part == 0 and offset > 0:
                            relative_part = current_part
                            if relative_part == 0:
                                if part_count == 1:
                                    data = chunk[first_part_cut:last_part_cut]
                                else:
                                    data = chunk[first_part_cut:]
                            elif relative_part == part_count - 1:
                                data = chunk[:last_part_cut]
                            else:
                                data = chunk
                        else:
                            absolute_offset_part = current_part
                            if part_count == 1:
                                data = chunk[first_part_cut:last_part_cut]
                            elif absolute_offset_part == 0:
                                data = chunk[first_part_cut:]
                            elif absolute_offset_part == part_count - 1:
                                data = chunk[:last_part_cut]
                            else:
                                data = chunk

                        if bytes_yielded_total + len(data) > req_length:
                            remaining = req_length - bytes_yielded_total
                            yield data[:remaining]
                            bytes_yielded_total += remaining
                            break
                        else:
                            yield data
                            bytes_yielded_total += len(data)

                        current_part += 1

                        if bytes_yielded_total >= req_length:
                            break

                    if bytes_yielded_total >= req_length:
                        break

                LOGGER.info(
                    "Download complete: file=%s bytes_sent=%s", file_name, bytes_yielded_total
                )
            except Exception as e:
                LOGGER.error("Error during download: file=%s error=%s", file_name, e)
                raise
            finally:
                try:
                    await transferrer._cleanup()
                except Exception:
                    pass
                LOGGER.info("ParallelTransferrer cleaned up for file=%s", file_name)

        return StreamingResponse(
            file_generator(),
            headers=headers,
            status_code=206 if range_header else 200,
            media_type=mime_type,
        )


@app.exception_handler(HTTPException)
async def http_error(request: Request, exc: HTTPException):
    error_message = error_messages.get(exc.status_code)
    return Response(
        content=exc.detail or error_message, status_code=exc.status_code
    )


api_instance = FileToLinkAPI(
    api_id=Telegram.API_ID,
    api_hash=Telegram.API_HASH,
    bot_token=Telegram.BOT_TOKEN,
)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "__main__:app",
        host=Server.BIND_ADDRESS,
        port=Server.PORT,
        workers=1,
        loop="uvloop",
        limit_concurrency=5000,
        backlog=8192,
        timeout_keep_alive=120,
        h11_max_incomplete_event_size=16777216,
    )