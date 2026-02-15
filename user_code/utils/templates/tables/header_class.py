import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle

class SectionHeader:
    def __init__(
        self,
        ax,
        number: str,
        title: str,
        color_left="#e6007e",
        color_right="#111111",
        color_text="white",
        border_color="black"
    ):
        self.ax = ax
        self.number = number
        self.title = title
        self.color_left = color_left
        self.color_right = color_right
        self.color_text = color_text
        self.border_color = border_color

    def draw(self):
        self.ax.axis('off')

        # Общий прямоугольник с рамкой (вся ширина, весь заголовок)
        self.ax.add_patch(Rectangle(
            (0, 0), 1, 1,
            transform=self.ax.transAxes,
            facecolor=self.color_right,
            edgecolor=self.border_color,
            linewidth=1,
            zorder=0
        ))

        # Цветной левый блок под номер
        self.ax.add_patch(Rectangle(
            (0, 0), 0.07, 1,
            transform=self.ax.transAxes,
            facecolor=self.color_left,
            linewidth=0,
            zorder=1
        ))

        # Номер
        self.ax.text(
            0.035, 0.5,
            self.number,
            color=self.color_text,
            weight='bold',
            ha='center', va='center',
            transform=self.ax.transAxes,
            fontsize=10,
            zorder=2
        )

        # Текст заголовка
        self.ax.text(
            0.08, 0.5,
            self.title,
            color=self.color_text,
            weight='bold',
            ha='left', va='center',
            transform=self.ax.transAxes,
            fontsize=10,
            zorder=2
        )
