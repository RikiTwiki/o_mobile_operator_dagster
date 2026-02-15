import smtplib
import requests
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage

from email.message import EmailMessage


class MailerBase(EmailMessage):
    """
    Базовый класс для отправки HTML-писем с inline-изображениями через SMTP.
    """
    host: str = "nurbox.nurtelecom.kg"
    smtp_auth: bool = False
    username: str = "nod@nurtelecom.kg"
    password: str = "SECRET"
    smtp_secure: str = "tls"
    smtp_port: int = 25
    smtp_options: dict = {"verify": False}

    charset: str = "utf-8"
    mail_from: str = "nod@nurtelecom.kg"
    from_name: str = "NUR OPEN DATA"
    subject: str = "Subject"
    html_body: str = "<b>HTML</b> body"
    alt_body: str = "Alternative non-html body"

    def __init__(self, *args, **kwargs):

        print(f"[DEBUG] MailerBase.__init__ got kwargs={kwargs!r}")

        super().__init__(*args, **kwargs)

        # Собираем структуру письма: related -> alternative -> (plain, html)
        self.msg = MIMEMultipart('mixed')
        self.msg['From'] = f"{self.from_name} <{self.mail_from}>"
        self.msg['Subject'] = self.subject

        # Альтернативный контейнер для plain и html
        alt = MIMEMultipart('alternative')
        alt.attach(MIMEText(self.alt_body, 'plain', self.charset))
        alt.attach(MIMEText(self.html_body, 'html', self.charset))
        self.msg.attach(alt)

    def link_image(self, data: str, cid_name: str) -> str:
        """
        Аналог PHP addStringEmbeddedImage:
        - data: путь к файлу или URL
        - cid_name: идентификатор вложения (возвращается для <img src="cid:...">)
        """
        try:
            if data.startswith(("http://", "https://")):
                resp = requests.get(data)
                resp.raise_for_status()
                img_data = resp.content
            else:
                with open(data, "rb") as f:
                    img_data = f.read()

            mime_img = MIMEImage(img_data, _subtype="png")


            mime_img.add_header("Content-ID", f"<{cid_name}>")
            mime_img.add_header("Content-Disposition", "inline", filename=f"{cid_name}.png")

            self.msg_root.attach(mime_img)

        except Exception as e:

            print(f"Error embedding image: {e}")

        return cid_name

    def init(self) -> None:
        """
        Обновляет альтернативное содержимое (plain/html) в письме.
        """
        # Извлекаем вложения кроме альтернативного
        parts = [p for p in self.msg.get_payload() if p.get_content_maintype() != 'multipart']
        alt = MIMEMultipart('alternative')
        alt.attach(MIMEText(self.alt_body, 'plain', self.charset))
        alt.attach(MIMEText(self.html_body, 'html', self.charset))
        # Устанавливаем новые вложения: альтернативное сначала, затем остальные
        self.msg.set_payload([alt] + parts)

    def send(self) -> None:
        """
        Отправляет сообщение через SMTP.
        """
        try:
            with smtplib.SMTP(self.host, self.smtp_port) as server:
                if self.smtp_secure.lower() == 'tls':
                    server.starttls()
                if self.smtp_auth:
                    server.login(self.username, self.password)
                server.send_message(self.msg)
        except Exception as e:
            capture_exception(e)
            raise
