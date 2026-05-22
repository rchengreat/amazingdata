import os
import AmazingData as ad
from dotenv import load_dotenv

load_dotenv()


def get_client():
    """登录并返回 AmazingData 模块（登录后所有接口均可用）"""
    username = os.environ["AD_USERNAME"]
    password = os.environ["AD_PASSWORD"]
    host = os.environ["AD_HOST"]
    port = int(os.environ["AD_PORT"])

    # Call internal set_cfg with force_logout=True before login.
    # This tells the SDK to kick any existing session for this user
    # during the login handshake, providing best-effort recovery from
    # stale sessions left by previously killed containers.
    try:
        _set_cfg = ad.login.__globals__.get("set_cfg")
        if _set_cfg is not None:
            _set_cfg(username, password, host, port, force_logout=True)
    except Exception:
        pass

    ad.login(
        username=username,
        password=password,
        host=host,
        port=port,
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
