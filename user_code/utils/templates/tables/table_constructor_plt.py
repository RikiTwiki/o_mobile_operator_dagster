import math
import matplotlib.pyplot as plt

class TableConstructor:
    """
    Python version of the PHP TableConstructor, rendering tables with matplotlib.
    """
    def __init__(self, font_size=12):
        self.header = []      # list of header labels
        self.rows = []        # list of data rows (each a list of cell values)
        self.comments = []    # optional comments to display above table
        self.font_size = font_size

    def add_header(self, labels):
        """
        Add table header labels.
        :param labels: list of column label strings
        """
        self.header = labels

    def add_row(self, cell_values, style_if=None, minmax=None):
        """
        Add a data row with optional conditional styling or gradient fill.
        :param cell_values: list of cell values (numbers or strings)
        :param style_if: dict with keys 'plan', 'operator', 'color' for threshold styling
        :param minmax: dict with keys 'min', 'max' for gradient fill
        """
        self.rows.append({
            'values': cell_values,
            'style_if': style_if,
            'minmax': minmax
        })

    def add_comment(self, text):
        """
        Add a comment box above the table.
        """
        self.comments.append(text)

    def style_if(self, fact, plan, operator, color):
        """
        Return color if fact compared to plan by operator passes.
        :param fact: numeric value
        :param plan: numeric threshold
        :param operator: one of '>=', '<=', '<', '>', '='
        :param color: hex string e.g. '#FF0000'
        :return: color string or None
        """
        f = round(fact)
        p = round(plan)
        ok = False
        if operator == '>=' and f >= p:
            ok = True
        elif operator == '<=' and f <= p:
            ok = True
        elif operator == '<' and f < p:
            ok = True
        elif operator == '>' and f > p:
            ok = True
        elif operator == '=' and fact == plan:
            ok = True
        return color if ok else None

    def generate_color_style(self, value, vmin, vmax):
        """
        Return a gradient color between white and #FF4D6E based on log scale.
        :param value: numeric
        :param vmin: minimum value
        :param vmax: maximum value
        :return: hex color string
        """
        if vmax == vmin:
            return '#FFFFFF'
        log_min = math.log(vmin + 1)
        log_max = math.log(vmax + 1)
        log_val = math.log(value + 1)
        pct = (log_val - log_min) / (log_max - log_min)
        pct = max(0.0, min(1.0, pct))
        # gradient from white to #FF4D6E
        r_min, g_min, b_min = 0xFF, 0xFF, 0xFF
        r_max, g_max, b_max = 0xFF, 0x4D, 0x6E
        r = int(r_min + (r_max - r_min) * pct)
        g = int(g_min + (g_max - g_min) * pct)
        b = int(b_min + (b_max - b_min) * pct)
        return f"#{r:02X}{g:02X}{b:02X}"

    def render(self, figsize=(8, 2), save_path=None):
        """
        Render the table using matplotlib.
        :param figsize: tuple for figure size
        :param save_path: if provided, save figure to this path
        :return: matplotlib Figure
        """
        # Prepare data and colors
        data = [row['values'] for row in self.rows]
        n_rows = len(data)
        n_cols = len(self.header)

        # Build cell background colors for data rows
        cell_colours = []
        for row in self.rows:
            colours = []
            for i, val in enumerate(row['values']):
                # default white
                bg = '#FFFFFF'
                # gradient fill
                if row['minmax'] is not None:
                    bg = self.generate_color_style(
                        float(val),
                        row['minmax']['min'],
                        row['minmax']['max']
                    )
                # threshold styling
                if row['style_if'] is not None:
                    style = row['style_if']
                    custom = self.style_if(
                        float(val),
                        style['plan'],
                        style['operator'],
                        style['color']
                    )
                    if custom:
                        bg = custom
                colours.append(bg)
            cell_colours.append(colours)

        # Header background and text color
        header_bg = '#43484c'
        header_colors = [header_bg] * n_cols

        # Create figure and axis
        fig, ax = plt.subplots(figsize=figsize)
        ax.axis('off')

        # Render comment(s)
        if self.comments:
            comment_text = "\n".join(self.comments)
            fig.text(0.01, 0.98, comment_text, va='top', fontsize=self.font_size + 2)

        # Create table
        tbl = ax.table(
            cellText=data,
            colLabels=self.header,
            cellColours=cell_colours,
            colColours=header_colors,
            cellLoc='center',
            loc='center'
        )
        tbl.auto_set_font_size(False)
        tbl.set_fontsize(self.font_size)
        tbl.scale(1, 1)

        fig.tight_layout()
        if save_path:
            fig.savefig(save_path, bbox_inches='tight')
        return fig

# Example usage:
# tc = TableConstructor()
# tc.add_header(['A', 'B', 'C'])
# tc.add_row([1, 2, 3], minmax={'min':1,'max':3})
# tc.add_row([5, 1, 4], style_if={'plan':3,'operator':'>=','color':'#FF0000'})\# tc.add_comment('Sample comment')
# fig = tc.render()