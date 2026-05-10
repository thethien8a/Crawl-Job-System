from .browser import VietnamWorksBrowser
from .utils import silence_asyncio_windows_proactor_error
import sys


def pretty_html(html: str) -> str:
    indent = "  "
    level = 0
    result = []
    i = 0
    n = len(html)
    void_tags = {"area", "base", "br", "col", "embed", "hr", "img",
                 "input", "link", "meta", "param", "source", "track", "wbr"}
    while i < n:
        if html[i] == "<":
            j = html.find(">", i)
            if j == -1:
                break
            tag = html[i:j + 1]
            if tag.startswith("</"):
                level = max(0, level - 1)
                result.append(indent * level + tag)
            elif tag.startswith("<!--") or tag.startswith("<!") or tag.startswith("<?"):
                result.append(indent * level + tag)
            elif tag.endswith("/>"):
                result.append(indent * level + tag)
            else:
                result.append(indent * level + tag)
                tag_name = tag[1:].split()[0].split(">")[0].lower()
                if tag_name not in void_tags:
                    level += 1
            i = j + 1
        else:
            j = html.find("<", i)
            if j == -1:
                j = n
            text = html[i:j].strip()
            if text:
                result.append(indent * level + text)
            i = j
    return "\n".join(result)


async def main():
    async with VietnamWorksBrowser() as browser:
        html = await browser.get_job_detail_html("https://www.vietnamworks.com/chuyen-vien-quan-tri-co-so-du-lieu-va-ung-dung-cntt-database-administrator-dba-ma-so-vhqt-01-2030615-jv?source=searchResults&searchType=2&placement=2030615&sortBy=date&qs=0")
        with open("test.html", "w", encoding="utf-8") as f:
            if html:
                f.write(pretty_html(html))

if __name__ == "__main__":
    import asyncio
    
    silence_asyncio_windows_proactor_error()
    
    # Workaround for Windows asyncio ProactorBasePipeTransport ValueError
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        
    try:
        asyncio.run(main())
    except Exception as e:
        pass

