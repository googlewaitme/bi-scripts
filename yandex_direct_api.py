from tapi_yandex_direct import YandexDirect


class DirectApi():
    def __init__(self, token, login):
        self.token = token
        self.login = login
        self.client = YandexDirect(
            # Required parameters:
            access_token=self.token,
            # If you are making inquiries from an agent account, you must be sure to specify the account login.
            login=self.login,
            # Enable sandbox.
            is_sandbox=False,
            # Repeat request when units run out
            retry_if_not_enough_units=False,
            # The language in which the data for directories and errors will be returned.
            language="ru",
            # Repeat the request if the limits on the number of reports or requests are exceeded.
            retry_if_exceeded_limit=True,
            # Number of retries when server errors occur.
            retries_if_server_error=5
        )

    def get_report_by_range(self, start_day: str, end_day: str) -> str:
        """
        start_day: isoformat
        end_day: isoformat
        """
        request_body = {
            "params": {
                "SelectionCriteria": {
                    "DateFrom": start_day,
                    "DateTo": end_day
                },
                "FieldNames": ["Date", "Cost", "Impressions", "Clicks", "CampaignId"],
                "OrderBy": [{
                    "Field": "Date"
                }],
                "ReportName": "Actual Data",
                "ReportType": "CAMPAIGN_PERFORMANCE_REPORT",
                "DateRangeType": "CUSTOM_DATE",
                "Format": "TSV",
                "IncludeVAT": "YES",
                "IncludeDiscount": "YES"
            }
        }
        report = self.client.reports().post(data=request_body)
        return report.data