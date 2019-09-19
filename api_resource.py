# -*- coding: utf-8 -*-
import pandas as pd

from AlationInstance import AlationInstance
import json

api_json = """
{
  "path": [
    "New Flightaware APIs",
    "Weather",
    "Get METAR"
  ],
  "request_type": "GET",
  "input_schema": {
    "properties": {
      "airport": {
        "type": "string",
        "examples": [
          "KJFK"
        ],
        "title" : "Airport ICAO (input)",
        "description": "ICAO code of the airport for which you want the weather"
      }
    }
  },
  "resource_url": "https:/flightxml.flightaware.com/json/FlightXML2/MetarEx",
  "output_schema": {
    "type": "object",
    "properties": {
      "MetarExResult": {
        "type": "object",
        "properties": {
          "metar": {
            "items": {
              "properties": {
                "wind_speed": {
                  "title" : "Wind speed in knots",
                  "description" : "Wind speed in knots",
                  "type": "integer",
                  "examples": [
                    "11"
                  ]
                },
                "cloud_type": {
                  "title" :  "Cloud Type",
                  "description" : "a coded reference to cloud types",
                  "type": "string",
                  "examples": [
                    "SCT"
                  ]
                },
                "cloud_altitude": {
                  "title" : "Cloud Altitude",
                  "description" : "Cloud altitude in feet above ground",
                  "type": "integer",
                  "examples": [
                    "1400"
                  ]
                },
                "cloud_friendly": {
                  "type": "string",
                  "title" : "Clouds in plain english",
                  "description" : "A plain english explanation of cloud conditions",
                  "examples": [
                    "Scattered clouds"
                  ]
                },
                "wind_direction": {
                  "title" : "Wind direction",
                  "description" : "Wind direction in degrees magnetic",
                  "type": "integer",
                  "examples": [
                    "200"
                  ]
                },
                "temp_relhum": {
                  "title" : "Relative humidity of the air",
                  "description" : "This is a percentage",
                  "type": "integer",
                  "examples": [
                    "89"
                  ]
                },
                "visibility": {
                  "title" : "Visibility",
                  "description" : "Visibility in km or sm",
                  "type": "integer",
                  "examples": [
                    "10"
                  ]
                },
                "pressure": {
                  "title" : "Barometric Pressure",
                  "description" : "In inches or millibar",
                  "type": "number",
                  "examples": [
                    "30.1700000763"
                  ]
                },
                "airport": {
                  "title" : "Airport code (ICAO)",
                  "description" : "Should be identical to the input",
                  "type": "string",
                  "examples": [
                    "KJFK"
                  ]
                },
                "wind_speed_gust": {
                  "title" : "Wind speed of the gusts",
                  "description" : "If gusts are present",
                  "type": "integer",
                  "examples": [
                    "0"
                  ]
                },
                "time": {
                  "title" : "Time of the observation",
                  "description" : "This is in seconds (Unix)",
                  "type": "integer",
                  "examples": [
                    "1533300660"
                  ]
                },
                "raw_data": {
                  "title" : "Raw text of METAR",
                  "description" : "Raw data as provided by the Met Office",
                  "type": "string",
                  "examples": [
                    "KJFK 031251Z 20011KT 10SM SCT014 26/24 A3017 RMK AO2 SLP215 T02610239"
                  ]
                },
                "wind_friendly": {
                  "title" : "Wind description",
                  "description" : "Strength of the wind in plain english",
                  "type": "string",
                  "examples": [
                    "Windy"
                  ]
                },
                "conditions": {
                  "title" : "Runway conditions",
                  "description" : "Description of the runway, for example wet",
                  "type": "string"
                },
                "temp_dewpoint": {
                  "title" : "Temperature of Dewpoint",
                  "description" : "Relevant for visibility, given in (C)",
                  "type": "integer",
                  "examples": [
                    "24"
                  ]
                },
                "temp_air": {
                  "title" : "Air Temperature",
                  "description" : "Given in Degrees Celsius (C)",
                  "type": "integer",
                  "examples": [
                    "26"
                  ]
                }
              }
            },
            "type": "array"
          },
          "next_offset": {
            "title" : "Next offset",
            "description" : "Technical offset for Flightaware API",
            "type": "integer",
            "examples": [
              "1"
            ]
          }
        }
      }
    }
  }
}
"""





if __name__ == "__main__":
    # -- API Resources

    alation_1 = AlationInstance("https://demo-sales-se.alationcatalog.com",
        "matthias.funke@alation.com", "...")

    r = alation_1.post_api_resource(json.loads(api_json))
    print (r)

