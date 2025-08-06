import smtplib
from email.message import EmailMessage
import os
import logging
from Cycle_Count_result_Reports.Script.time_utils import get_report_display_date
from Cycle_Count_result_Reports.config.settings import EXPORT_DIR

logger = logging.getLogger(__name__)

def send_email_with_attachments(subject, body, sender_email, recipient_emails, cc_emails,smtp_server, smtp_port):
    try:
        logger.info("Email function entered.")
        logger.info("Preparing email to send reports...")

        msg = EmailMessage()
        msg['Subject'] = subject
        msg['From'] = sender_email
        msg['To'] = ", ".join(recipient_emails)
        msg['Cc'] = ", ".join(cc_emails)
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

        all_recipients = recipient_emails + cc_emails
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.sendmail(sender_email, all_recipients, msg.as_string())
            logger.info(f"Email sent to: {recipient_emails}, CC: {cc_emails}")

    except Exception as e:
        logger.error(f"Error while sending email: {str(e)}")
