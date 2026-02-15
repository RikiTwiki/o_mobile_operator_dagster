import html
from datetime import datetime
from typing import Optional
import zoneinfo


class HtmlReportPage:
    HEADER_CONFIGS = {
        "nod": {
            "logo_url": "https://cca.o.kg/logo/logo.png",
            "logo_width": 50,
            "logo_alt": "NODLogo",
            "h1": "NUR OPEN DATA",
            "h1_width": 130,
        },
        "saima": {
            "logo_url": "https://cca.o.kg/logo/logo_saima.png",
            "logo_width": 180,
            "logo_alt": "SaimaLogo",
            "h1": "",
            "h1_width": 0,
        },
    }

    def __init__(self, title: str = "", render_contents: bool = True, tz: Optional[str] = None):
        self.title = title
        self.render_contents = render_contents
        self.tz = zoneinfo.ZoneInfo(tz) if tz else zoneinfo.ZoneInfo("UTC")
        self.header_html = ""
        self.body_html = ""
        self.contents_html = ""
        self.contents = []

        self.block_number = 0
        self.module_number = 0
        self.module_child_number = 0

        self.block_title = ""
        self.module_title = ""
        self.module_child_title = ""

    def set_header(self, title: str, type_: str = "nod") -> None:
        self.title = title
        config = self.HEADER_CONFIGS.get(type_, self.HEADER_CONFIGS["nod"])
        now = datetime.now(self.tz)
        hour = now.hour
        if 0 <= hour < 6:
            greet = "ЗДРАВСТВУЙТЕ!"
        elif hour < 11:
            greet = "ДОБРОЕ УТРО!"
        elif hour < 17:
            greet = "ДОБРЫЙ ДЕНЬ!"
        else:
            greet = "ДОБРЫЙ ВЕЧЕР!"

        logo_img_width = max(0, config["logo_width"] - 20)

        header = f"""
            <table style="width:100%;border-collapse:collapse;margin-bottom:8px;">
                <tr>
                    <td style="background-color:#0a0a0a;width:{config['logo_width']}px;padding:5px 10px;vertical-align:middle;">
                        <img src="{config['logo_url']}" alt="{config['logo_alt']}" width="{logo_img_width}" style="display:block;">
                    </td>
        """

        if config["h1_width"]:
            header += f"""
                <td style="background-color:#0a0a0a;width:{config['h1_width']}px;color:#fff;vertical-align:middle;">
                    <b>{html.escape(config['h1'])}</b>
                </td>
            """

        header += f"""
                    <td style="background-color:#0a0a0a;color:#fff;vertical-align:middle;text-align:center;padding-right:170px;">
                        <div style="font-size:14px;">
                            <div><b>{greet}</b></div>
                            <div style="margin-top:4px;"><b>{html.escape(title)}</b></div>
                            <div style="margin-top:2px;font-size:12px;">обновлено: {now.strftime('%Y-%m-%d %H:%M')}</div>
                        </div>
                    </td>
                </tr>
            </table>
            <br>
        """

        self.header_html = header

    def add_block_header(self, name: str) -> None:
        self.block_title = name
        self.block_number += 1
        self.module_number = 0
        self.module_child_number = 0
        number = str(self.block_number)
        header_data = f"""
            <a name="link-{number}"></a>
            <table style="width:100%;border-collapse:collapse;margin-top:16px;margin-bottom:4px;">
                <tr>
                    <td style="background-color:#e2007a;color:#ffffff;width:50px;text-align:center;padding:6px;">
                        <b>{number}</b>
                    </td>
                    <td style="background-color:#0a0a0a;color:#ffffff;padding:6px;text-align:left;">
                        <b>{html.escape(name)}</b>
                    </td>
                </tr>
            </table>
        """
        self.body_html += header_data
        self._add_to_contents("block", number, name)

    def add_module_header(self, name: str, target: str = "") -> None:
        self.module_title = name
        self.module_number += 1
        self.module_child_number = 0
        number = f"{self.block_number}.{self.module_number}"
        target_html = f"<br><i>{html.escape(target)}</i>" if target else ""
        header_data = f"""
            <a name="link-{number}"></a>
            <table style="width:100%;border-collapse:collapse;margin-top:8px;margin-bottom:4px;">
                <tr>
                    <td style="background-color:#0a0a0a;color:#ffffff;width:50px;text-align:center;padding:6px;">
                        <b>{number}</b>
                    </td>
                    <td style="background-color:#e3e3e3;color:#0a0a0a;padding:6px;text-align:left;">
                        <b>{html.escape(name)}</b>{target_html}
                    </td>
                    <td style="background-color:#e3e3e3;color:#0a0a0a;width:70px;text-align:center;padding:6px;">
                        <a href="#link_contents" style="text-decoration:none;color:#0a0a0a;font-size:12px;">наверх</a>
                    </td>
                </tr>
            </table>
        """
        self.body_html += header_data
        self._add_to_contents("module", number, name)

    def add_module_child_header(self, name: str, target: str = "") -> None:
        self.module_child_title = name
        self.module_child_number += 1
        number = f"{self.block_number}.{self.module_number}.{self.module_child_number}"
        target_html = f"<br><i>{html.escape(target)}</i>" if target else ""
        header_data = f"""
            <a name="link-{number}"></a>
            <table style="width:100%;border-collapse:collapse;margin-top:4px;margin-bottom:4px;">
                <tr>
                    <td style="background-color:#43484c;color:#ffffff;width:50px;text-align:center;padding:6px;">
                        <b>{number}</b>
                    </td>
                    <td style="background-color:#f5f5f5;color:#43484c;padding:6px;text-align:left;">
                        <b>{html.escape(name)}</b>{target_html}
                    </td>
                    <td style="background-color:#f5f5f5;color:#43484c;width:70px;text-align:center;padding:6px;">
                        <a href="#link_contents" style="text-decoration:none;color:#43484c;font-size:12px;">наверх</a>
                    </td>
                </tr>
            </table>
        """
        self.body_html += header_data
        self._add_to_contents("moduleChild", number, name)

    def add_text(self, text: str, tag: str = "div") -> None:
        safe = html.escape(text)
        self.body_html += f"<{tag} style='margin:6px 0;font-size:16px;'><b>{safe}</b></{tag}><br>"

    def add_custom_html(self, html_fragment: str) -> None:
        self.body_html += html_fragment

    def add_image(self, src: str, href: Optional[str] = None, alt: str = "image") -> None:
        img_tag = f"<img alt='{html.escape(alt)}' src='{html.escape(src)}' style='max-width:100%;display:block;margin:8px 0;'>"
        if href:
            img_tag = f"<a href='{html.escape(href)}' style='text-decoration:none;'>{img_tag}</a>"
        self.body_html += f"<div>{img_tag}</div>"

    def _add_to_contents(self, type_: str, number: str, text: str) -> None:
        self.contents.append({"type": type_, "number": number, "text": text})

    def _build_contents(self) -> None:
        if not self.render_contents or not self.contents:
            self.contents_html = ""
            return
        rows = ""
        for item in self.contents:
            if item["type"] == "block":
                tr = f"""
                  <tr>
                    <td style="background-color:#0a0a0a;color:#fff;text-align:center;padding:3px;width:30px;">
                      <b>{item['number']}</b>
                    </td>
                    <td style="background-color:#0a0a0a;text-align:left;padding:3px 6px;" colspan="3">
                      <a href="#link-{item['number']}" style="text-decoration:none;color:#fff;"><b>{html.escape(item['text'])}</b></a>
                    </td>
                  </tr>"""
            elif item["type"] == "module":
                tr = f"""
                  <tr>
                    <td style="background-color:#43484c;width:30px;"></td>
                    <td style="background-color:#43484c;color:#fff;text-align:center;padding:3px;width:30px;">
                      <b>{item['number']}</b>
                    </td>
                    <td style="background-color:#43484c;text-align:left;padding:3px 6px;" colspan="2">
                      <a href="#link-{item['number']}" style="text-decoration:none;color:#fff;"><b>{html.escape(item['text'])}</b></a>
                    </td>
                  </tr>"""
            elif item["type"] == "moduleChild":
                tr = f"""
                  <tr>
                    <td style="background-color:#f5f5f5;width:30px;"></td>
                    <td style="background-color:#f5f5f5;width:30px;"></td>
                    <td style="background-color:#f5f5f5;color:#43484c;text-align:center;padding:3px;width:35px;">
                      {item['number']}
                    </td>
                    <td style="background-color:#f5f5f5;text-align:left;padding:3px 6px;">
                      <a href="#link-{item['number']}" style="text-decoration:none;color:#43484c;"><i>{html.escape(item['text'])}</i></a>
                    </td>
                  </tr>"""
            else:
                tr = ""
            rows += tr
        self.contents_html = f"""
            <a name="link_contents"></a>
            <table style="border-collapse:collapse;width:60%;margin-bottom:12px;font-size:12px;">
                <tr>
                    <td colspan="4" style="background-color:#e2007a;color:#fff;text-align:center;padding:6px;font-size:13px;">
                        <b>Содержание отчёта</b>
                    </td>
                </tr>
                {rows}
            </table>
            <br>
        """

    def render(self) -> str:
        self._build_contents()
        title_escaped = html.escape(self.title or "Отчёт")
        now = datetime.now(self.tz)
        html_doc = f"""<!doctype html>
<html lang="ru">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>{title_escaped}</title>
    <style>
      body {{
        font-family: Calibri, Candara, Segoe, 'Segoe UI', Optima, Arial, sans-serif;
        margin: 0;
        padding: 16px;
        background: #f9f9f9;
        color: #1f1f1f;
        line-height: 1.3;
      }}
      a {{ color: inherit; }}
      .container {{ max-width: 1200px; margin: auto; background: #fff; padding: 16px; box-shadow: 0 4px 14px rgba(0,0,0,0.05); border-radius: 12px; }}
      .section {{ margin-bottom: 24px; }}
      .small {{ font-size: 12px; color: #555; }}
    </style>
  </head>
  <body>
    <div class="container">
      {self.header_html}
      {self.contents_html}
      <div class="section">
        {self.body_html}
      </div>
      <div class="small">Сформировано: {now.strftime('%Y-%m-%d %H:%M')}</div>
    </div>
  </body>
</html>"""
        return html_doc

    def disable_contents(self) -> None:
        self.render_contents = False

    def enable_contents(self) -> None:
        self.render_contents = True