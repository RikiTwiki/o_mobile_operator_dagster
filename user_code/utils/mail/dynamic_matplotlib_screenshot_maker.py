import numpy as np
from scipy.interpolate import make_interp_spline
from datetime import datetime
from matplotlib.ticker import MultipleLocator
import matplotlib.pyplot as plt
import io
import base64
from typing import List, Dict, Any, Optional


class DynamicMatplotlibScreenshotMaker:
    """
    Универсальный рендерщик: рисует текст и сохраняет в PNG.
    Можно использовать как заглушку или дебаг-вывод.

    Атрибуты можно менять до или во время вызова get_png().
    """

    def __init__(
            self,
            data: str = "Данные для скриншота",
            width: int = 1350,
            height: int = 400,
            dpi: int = 100,
            wait_time: float = 1.0
    ):
        self.data = data
        self.width = width
        self.height = height
        self.dpi = dpi
        self.wait_time = wait_time

    def get_png(
            self,
            data: Optional[str] = None,
            width: Optional[int] = None,
            height: Optional[int] = None,
            dpi: Optional[int] = None,
            render_func: Optional[callable] = None
    ) -> bytes:
        """
        Рисует график или текст на изображении PNG.

        :param data: Текст для отрисовки (если не используется render_func)
        :param width: Ширина холста
        :param height: Высота холста
        :param dpi: DPI
        :param render_func: Функция для кастомной отрисовки, принимающая ax как параметр
        :return: PNG данные как bytes
        """
        import time

        w = width or self.width
        h = height or self.height
        d = dpi or self.dpi

        time.sleep(self.wait_time)

        fig = plt.figure(figsize=(w / d, h / d), dpi=d)
        fig.patch.set_facecolor('white')

        if render_func:
            ax = fig.add_subplot(111)
            render_func(ax)
        else:
            ax = fig.add_axes((0, 0, 1, 1))
            ax.axis('off')
            text = data or self.data
            ax.text(0.5, 0.5, text, ha='center', va='center', fontsize=14, wrap=True)

        plt.tight_layout()

        buf = io.BytesIO()
        fig.savefig(buf, format='png', bbox_inches='tight', pad_inches=0.1)
        plt.close(fig)
        buf.seek(0)

        return buf.getvalue()