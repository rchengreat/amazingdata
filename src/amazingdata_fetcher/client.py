import atexit
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

    # Ensure the session is released when the process exits normally.
    atexit.register(_logout_once)

    return ad


_logged_out = False


def _logout_once():
    global _logged_out
    if not _logged_out:
        _logged_out = True
        try:
            ad.logout()
        except Exception:
            pass
