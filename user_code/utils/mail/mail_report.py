import base64
import mimetypes
import os
import urllib
import uuid
from datetime import datetime
from typing import Optional, List, Dict, Union
from email.mime.image import MIMEImage

from utils.mail.mailer_base import MailerBase


class MailReport(MailerBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.render_contents: bool = False
        self.basic_header: str = ""
        self.contents: List[Dict[str, str]] = []
        self.contents_html: str = ""
        # NEW:
        self.image_mode = "cid"             # 'cid' | 'url' | 'file' | 'data'
        self.logo_file_override: str | None = None  # локальный путь к логотипу

    # NEW: единый генератор src
    def _img_src(self, ref: str, alt: str) -> str:
        mode = getattr(self, "image_mode", "cid")
        if mode == "cid":
            cid = self.link_image(ref, alt)  # ref здесь может быть URL — неважно
            return f"cid:{cid}"

        if mode == "file":
            path = self.logo_file_override or ref
            # защита от неверных путей
            if not os.path.isfile(path):
                raise FileNotFoundError(f"Logo file not found: {path}")
            with open(path, "rb") as f:
                b = f.read()
            mime = mimetypes.guess_type(path)[0] or "image/png"
            b64 = base64.b64encode(b).decode("ascii")
            return f"data:{mime};base64,{b64}"

        if mode == "data":
            with urllib.request.urlopen(ref, timeout=10) as r:
                b = r.read()
                mime = r.headers.get_content_type() or "image/png"
            b64 = base64.b64encode(b).decode("ascii")
            return f"data:{mime};base64,{b64}"

        # mode == "url"
        return ref

    def set_basic_header(self, title: str, type_: str = "nod") -> str:
        config = {
            'nod':  {'logo_url': "https://cca.o.kg/logo/logo.png", 'logo_width': 50,  'logo_alt': "NODLogo",  'h1': "NUR OPEN DATA", 'h1_width': 200},
            'saima':{'logo_url': "https://cca.o.kg/logo/logo_saima.png", 'logo_width': 180,'logo_alt': "SaimaLogo",'h1': "",             'h1_width': 0}
        }[type_]

        # hour = datetime.now().hour
        greet = 'ДОБРОЕ УТРО!'
        # if   0 <= hour < 6:  greet = 'ЗДРАВСТВУЙТЕ!'
        # elif 6 <= hour < 11: greet = 'ДОБРОЕ УТРО!'
        # elif 11 <= hour < 17:greet = 'ДОБРЫЙ ДЕНЬ!'
        # else:                greet = 'ДОБРЫЙ ВЕЧЕР!'

        # CHANGED: универсальный src вместо жесткого cid
        img_src = self._img_src(self.logo_file_override or config['logo_url'], config['logo_alt'])
        logo_img_width = max(1, config['logo_width'] - 20)

        header = (
            f"<table style=\"width:100%;border-collapse:collapse;font-family:Calibri, Candara, Segoe, 'Segoe UI', Optima, Arial, sans-serif;\">"
            f"<tr>"
            f"  <td style='background-color:#0a0a0a;width:{config['logo_width']}px;padding:5px 10px;vertical-align:middle;'>"
            f"    <img src='{img_src}' alt='{config['logo_alt']}' width='{logo_img_width}'/>"
            f"  </td>"
        )
        if config['h1_width']:
            header += (
                f"<td style='background-color:#0a0a0a;width:{config['h1_width']}px;color:#fff;vertical-align:middle;'>"
                f"  <b>{config['h1']}</b>"
                f"</td>"
            )
        header += (
            f"<td style='background-color:#0a0a0a;color:#fff;border:none;vertical-align:middle;text-align:center;padding-right:170px;'>"
            f"  <b>{greet}</b><br/><b>{title}</b>"
            f"</td>"
            f"</tr>"
            f"</table><br/>"
        )
        self.basic_header = header
        return header

    def add_block_header(self, name: str) -> str:
        self.blocks = getattr(self, 'blocks', 0) + 1
        self.modules = 0  # сброс модулей при новом блоке
        self.module_children = 0  # сброс дочерних модулей
        num = self.blocks
        header = (
            f"<a name='link-{num}'/>"
            f"<table style=\"width:100%;border-collapse:collapse;font-family:Calibri, Candara, Segoe, 'Segoe UI', Optima, Arial, sans-serif;\">"
            f"  <tr>"
            f"    <td style='background-color:#e2007a;color:#fff;"
            f"text-align:center;width:50px;'><b>{num}</b></td>"
            f"    <td style='background-color:#0a0a0a;color:#fff;"
            f"text-align:center;padding-right:30px;'><b>{name}</b></td>"
            f"  </tr>"
            f"</table>"
        )
        self._add_to_contents('block', str(num), name)
        return header

    def add_module_header(self, name: str, target: str = "") -> str:
        self.modules = getattr(self, 'modules', 0) + 1
        self.module_children = 0  # сброс дочерних при новом модуле
        idx = f"{self.blocks}.{self.modules}"
        target_html = f"<br/><i>{target}</i>" if target else ""
        header = (
            f"<a name='link-{idx}'/>"
            f"<table style=\"width:100%;border-collapse:collapse;font-family:Calibri, Candara, Segoe, 'Segoe UI', Optima, Arial, sans-serif;\">"
            f"  <tr>"
            f"    <td style='background-color:#0a0a0a;color:#fff;"
            f"text-align:center;width:50px;'><b>{idx}</b></td>"
            f"    <td style='background-color:#e3e3e3;color:#0a0a0a;"
            f"text-align:center;'><b>{name}</b>{target_html}</td>"
            f"    <td style='background-color:#e3e3e3;color:#0a0a0a;"
            f"text-align:center;width:70px;'>"
            f"      <a href='#link_contents' style='text-decoration:none;"
            f"color:#0a0a0a;'></a>"
            f"    </td>"
            f"  </tr>"
            f"</table>"
        )
        self._add_to_contents('module', idx, name)
        return header


    def add_module_child_header(self, name: str, target: str = "") -> str:
        self.module_children = getattr(self, 'module_children', 0) + 1
        idx = f"{self.blocks}.{self.modules}.{self.module_children}"
        target_html = f"<br/><i>{target}</i>" if target else ""
        header = (
            f"<a name='link-{idx}'/>"
            f"<table style=\"width:100%;border-collapse:collapse;font-family:Calibri, Candara, Segoe, 'Segoe UI', Optima, Arial, sans-serif;\">"
            f"  <tr>"
            f"    <td style='background-color:#43484c;color:#fff;"
            f"text-align:center;width:50px;'><b>{idx}</b></td>"
            f"    <td style='background-color:#f5f5f5;color:#43484c;"
            f"text-align:center;'><b>{name}</b>{target_html}</td>"
            f"    <td style='background-color:#f5f5f5;color:#43484c;"
            f"text-align:center;width:70px;'>"
            f"      <a href='#link_contents' style='text-decoration:none;"
            f"color:#43484c;'></a>"
            f"    </td>"
            f"  </tr>"
            f"</table>"
        )
        self._add_to_contents('moduleChild', idx, name)
        return header

    def add_content_to_statistic(self, content_html: str) -> str:
        fragment = content_html + "<br/>"
        return fragment

    def add_figure(self,
                   figure_bytes: str,
                   width_px: int|None = None,
                   height_px: int|None = None) -> str:
        if width_px is None and height_px is None:
            size = "max-width:100%;height:auto;"
        else:
            size = "width:100%; height:100%; display:block;"
        img_html = (
            f"<div>"
            f"  <img src='data:image/svg+xml;base64,{figure_bytes}'"
            f" style='{size}'/>"
            f"</div><br/>"
        )
        return img_html

    def _add_to_contents(self, type_: str, number: str, text: str):
        self.contents.append({"type": type_, "number": number, "text": text})

    def render_html_page(self) -> str:
        if self.render_contents:
            self._render_contents()
        return f"""<!doctype html>
            <html lang="ru">
            <head>
              <meta charset="UTF-8">
              <meta name="viewport" content="width=device-width,initial-scale=1.0">
              <style>
                body {{
                  font-family: Calibri, Candara, 'Segoe UI', Optima, Arial, sans-serif !important;
                  margin:0;
                  padding:0;
                }}
                table, th, td, a {{
                  font-family: Calibri, Candara, 'Segoe UI', Optima, Arial, sans-serif !important;
                }}
                a {{
                  color: #000 !important;
                }}
                table {{
                  border-collapse: collapse;
                  border-spacing: 0;
                  margin: 0;
                  padding: 0;
                  width: 100%;
                  background: transparent;
                  font-family: Calibri, Candara, 'Segoe UI', Optima, Arial, sans-serif;
                }}
                th, td {{
                  padding: 0 !important;
                  background: transparent;
                }}
              </style>
              <title>Mail</title>
            </head>
            <body>
              <div style="text-align:center;">
                {self.basic_header}
                {self.contents_html}
              </div>
            </body>
            </html>"""

    def _render_contents(self):
        if not self.contents:
            return
        rows = ""
        for item in self.contents:
            t, num, txt = item.values()
            if t == 'block':
                rows += (
                    f"<tr>"
                    f"<td style='background-color:#0a0a0a;color:#fff;"
                    f"text-align:center;width:30px;'><b>{num}</b></td>"
                    f"<td style='background-color:#0a0a0a;text-align:left;"
                    f"padding:3px 3px;' colspan='3'>"
                    f"<a href='#link-{num}' style='color:#fff;text-decoration:none;'>"
                    f"<b>{txt}</b></a></td>"
                    f"</tr>"
                )
            elif t == 'module':
                rows += (
                    f"<tr>"
                    f"<td style='background-color:#43484c;width:30px;'></td>"
                    f"<td style='background-color:#43484c;color:#fff;"
                    f"text-align:center;width:30px;'><b>{num}</b></td>"
                    f"<td style='background-color:#43484c;text-align:left;"
                    f"padding:3px 3px;' colspan='2'>"
                    f"<a href='#link-{num}' style='color:#fff;text-decoration:none;'>"
                    f"<b>{txt}</b></a></td>"
                    f"</tr>"
                )
            elif t == 'moduleChild':
                rows += (
                    f"<tr>"
                    f"<td style='background-color:#f5f5f5;width:30px;'></td>"
                    f"<td style='background-color:#f5f5f5;width:30px;'></td>"
                    f"<td style='background-color:#f5f5f5;color:#43484c;"
                    f"text-align:center;width:35px;'>{num}</td>"
                    f"<td style='background-color:#f5f5f5;text-align:left;"
                    f"padding:3px 3px;'>"
                    f"<a href='#link-{num}' style='color:#43484c;text-decoration:none;'>"
                    f"<i>{txt}</i></a></td>"
                    f"</tr>"
                )
        self.contents_html = (
            "<a name='link_contents'/>"
            "<table style=\"border-collapse:collapse;margin-bottom:8px;font-size:12px;width:60%;font-family:Calibri, Candara, Segoe, 'Segoe UI', Optima, Arial, sans-serif;\">"
            f"<tr><td colspan='4' style='background-color:#e2007a;color:#fff;"
            f"text-align:center;padding:4px;'><b>Содержание отчета</b></td></tr>"
            f"{rows}"
            "</table>"
        )

    def save_html_to_file(self, html: str, filename: str):
        with open(filename, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"✅ HTML сохранён в файл: {filename}")

from playwright.sync_api import sync_playwright
from PyPDF2 import PdfMerger
from dagster import get_dagster_logger

def html_to_pdf_page(html: str, output_path: str, scale: float = 1.0,    width: Optional[int] = None,
    height: Optional[int] = None):
    logger = get_dagster_logger()
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        page.set_content(html, wait_until="networkidle")

        page.add_style_tag(content="""
          html, body {
            margin: 0;
            padding: 0;
            background: transparent !important;
          }
        """)

        if width is None or height is None:
            if width is None:
                width = page.evaluate("() => document.documentElement.scrollWidth")
            if height is None:
                height = page.evaluate("() => document.body.scrollHeight")

                if height == 720:
                    height = page.evaluate("""
                                      () => {
                                        const tbl = document.querySelector('table:last-of-type');
                                        // bottom — расстояние от верха viewport до низа таблицы
                                        return Math.ceil(tbl.getBoundingClientRect().bottom);
                                      }
                                    """)
                    height += 50
                height += 7
        logger.info(f"width: {width}, height: {height}")
        page.set_viewport_size({"width": width, "height": height})

        page.pdf(
            path=output_path,
            print_background=True,
            height=f"{height*scale}px",
            width=f"{width*scale}px",
            margin={"top": "0px", "bottom": "0px", "left": "0px", "right": "0px"},
        )
        browser.close()

def merge_pdfs(input_paths: list[str], output_path: str):
    merger = PdfMerger()
    for path in input_paths:
        merger.append(path)
    with open(output_path, "wb") as f:
        merger.write(f)
    merger.close()