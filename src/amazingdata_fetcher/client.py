import os
import AmazingData as ad
from dotenv import load_dotenv

load_dotenv()


def get_client():
    """登录并返回 AmazingData 模块（登录后所有接口均可用）"""
    # Attempt to drop any existing session before opening a new one.
    # The SDK enforces a single-connection limit per user; a stale session
    # (e.g. a killed Docker container) would otherwise block the new login.
    try:
        ad.logout()
    except Exception:
        pass

    ad.login(
        username=os.environ["AD_USERNAME"],
        password=os.environ["AD_PASSWORD"],
        host=os.environ["AD_HOST"],
        port=int(os.environ["AD_PORT"]),
    )

    return ad


def logout():
    """显式释放 SDK 连接。每个脚本的 main() 应在 finally 块中调用此函数。
    注意：不要用 atexit 注册此函数——os._exit() 会绕过 atexit，导致连接泄漏。
    """
    try:
        ad.logout()
    except Exception:
        pass
