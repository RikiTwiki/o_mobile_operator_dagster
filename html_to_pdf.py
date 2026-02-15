from typing import Optional
from weasyprint import HTML, CSS

# базовые размеры в мм
_BASE_PAGE_SIZES = {
    "A4": (210, 297),
    "Letter": (216, 279),  # можно расширить при необходимости
}


class HtmlToPdf:
    def __init__(
        self,
        base_format: str = "A4",
        width_scale: float = 1.5,
        margins: Optional[dict] = None,
        base_url: Optional[str] = None,
    ):
        """
        :param base_format: базовый формат, например "A4" — от него берётся высота и ширина.
        :param width_scale: во сколько раз умножается ширина (по умолчанию 1.5 = +50%).
        :param margins: словарь с ключами top/right/bottom/left в CSS-единицах, например {"top":"15mm", ...}
        :param base_url: для относительных ссылок внутри HTML (изображения и т.п.).
        """
        if base_format not in _BASE_PAGE_SIZES:
            raise ValueError(f"Unknown base_format '{base_format}', supported: {list(_BASE_PAGE_SIZES.keys())}")
        base_w, base_h = _BASE_PAGE_SIZES[base_format]
        self.page_width_mm = base_w * width_scale
        self.page_height_mm = base_h  # оставляем высоту как в базовом формате
        self.margins = margins or {"top": "15mm", "right": "10mm", "bottom": "15mm", "left": "10mm"}
        self.base_url = base_url

    def _build_page_css(self) -> str:
        return f"""
        @page {{
            size: {self.page_width_mm:.0f}mm {self.page_height_mm:.0f}mm;
            margin: {self.margins['top']} {self.margins['right']} {self.margins['bottom']} {self.margins['left']};
        }}
        """

    def convert_to_file(self, html_string: str, output_path: str) -> str:
        """
        Преобразует переданный HTML в PDF и сохраняет в output_path. Возвращает путь.
        """
        page_css = self._build_page_css()
        stylesheet = CSS(string=page_css)
        doc = HTML(string=html_string, base_url=self.base_url)
        doc.write_pdf(target=output_path, stylesheets=[stylesheet])
        return output_path

    def convert_to_bytes(self, html_string: str) -> bytes:
        """
        Преобразует переданный HTML в PDF и возвращает байты.
        """
        page_css = self._build_page_css()
        stylesheet = CSS(string=page_css)
        doc = HTML(string=html_string, base_url=self.base_url)
        return doc.write_pdf(stylesheets=[stylesheet])