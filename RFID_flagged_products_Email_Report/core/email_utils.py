import smtplib
from email.message import EmailMessage
import os
import logging
from RFID_flagged_products_Email_Report.core.time_utils import get_report_display_date
from RFID_flagged_products_Email_Report.config.settings import EXPORT_DIR

logger = logging.getLogger(__name__)

def send_email_with_attachments(subject, body, sender_email, recipient_emails, smtp_server, smtp_port):
    try:
        logger.info("Email function entered.")
        logger.info("Preparing email to send reports...")

        msg = EmailMessage()
        msg['Subject'] = subject
        msg['From'] = sender_email
        msg['To'] = ", ".join(recipient_emails)
        msg.set_content(body)

        # Get today's date in the expected format
        today_str = get_report_display_date()

        attachments = []
        for filename in os.listdir(EXPORT_DIR):
            if filename.endswith('.xlsx') and today_str in filename and not filename.startswith('~$'):
                file_path = os.path.join(EXPORT_DIR, filename)
                with open(file_path, 'rb') as f:
                    file_data = f.read()
                    msg.add_attachment(
                        file_data,
                        maintype='application',
                        subtype='vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                        filename=filename
                    )
                    attachments.append(filename)

        if not attachments:
            logger.warning("No attachments found to send in email.")
        else:
            logger.info(f"Attachments added to email: {attachments}")

        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.send_message(msg)
            logger.info(f"Email sent successfully to {recipient_emails}")

    except Exception as e:
        logger.error(f"Error while sending email: {str(e)}")
