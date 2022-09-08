from typing import Any, List, Optional
from api.base import RestApiBase
from api.model import RestOperator


class WeatherApi(RestApiBase):
    """Visual Crossing Weather API

    Args:
        api_key (str): key used as URI param to authenticate on the API
    """

    def __init__(self, api_key: str):

        # This is an overkilled solution but I'm trying to show
        # what I would have done on a more complicated system
        # where you need retry policy, there is timeouts or other conditions

        self._OPERATIONS = {
            "get_timeline": RestOperator(
                url="https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{location}/{start_date}/{end_date}",
                method="get",
                allowed_params={"key", "include"},
                required_params={"key"},
                path_vars={"location", "start_date", "end_date"},
            ),
            "get_time": RestOperator(
                url="https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{location}/{start_date}",
                method="get",
                allowed_params={"key", "include"},
                required_params={"key"},
                path_vars={"location", "start_date"},
            ),
        }
        self.api_key = api_key
        super().__init__(req_timeout_s=5)

    def get_weather_timeline(
        self,
        location: str,
        start_date: int,
        end_date: Optional[int],
        hourly: bool = False,
    ) -> Any:
        """Get information about weather on at a specific location and a given window of time

        Args:
            location (str): ZIP code or a given lat and longitud (comma separated value). For example: `38.9697,-77.385`
            start_date (datetime): starting window date
            end_date (datetime): ending window date

        Returns:
            Any: Information of weather
        """

        if end_date is not None:
            if not hourly:
                return self._call_operation(
                    "get_timeline",
                    location=location,
                    start_date=start_date,
                    end_date=end_date,
                    key=self.api_key,
                    include="current",
                ).json()
            response: List[Any] = []
            for ts in range(start_date, end_date + 1, 3600):
                response.append(
                    self._call_operation(
                        "get_time",
                        location=location,
                        start_date=ts,
                        key=self.api_key,
                        include="current",
                    ).json()
                )
            return response

        return self._call_operation(
            "get_time",
            location=location,
            start_date=start_date,
            key=self.api_key,
            include="current",
        ).json()
