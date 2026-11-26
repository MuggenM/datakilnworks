"""Shared by the browser scripts: UI_SLOW=<factor> (default 1, CI uses 3) multiplies every fixed sleep and Playwright wait_for_timeout, because a shared CI runner
is several times slower than a workstation. `import _ui_slow` first thing in a script, then `_ui_slow.apply(page)` for each page."""
import os
import time

FACTOR = float(os.getenv("UI_SLOW", "1"))
if FACTOR != 1 and not getattr(time, "_ui_slow_patched", False):
    _sleep = time.sleep
    time.sleep = lambda s: _sleep(s * FACTOR)
    time._ui_slow_patched = True


def apply(page):
    if FACTOR != 1 and not getattr(page, "_ui_slow_patched", False):
        original = page.wait_for_timeout
        page.wait_for_timeout = lambda ms: original(ms * FACTOR)
        page._ui_slow_patched = True
    return page
