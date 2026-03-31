# Databricks notebook source
# COMMAND ----------
from jinja2 import Template

parameters = [
    {
        "table": "spotify_cata.silver.factstream",
        "alias": "factstream",
        "cols": "factstream.stream_id, factstream.listen_duration",
    },
    {
        "table": "spotify_cata.silver.dimuser",
        "alias": "dimuser",
        "cols": "dimuser.user_id, dimuser.user_name",
        "condition": "factstream.user_id = dimuser.user_id",
    },
    {
        "table": "spotify_cata.silver.dimtrack",
        "alias": "dimtrack",
        "cols": "dimtrack.track_id, dimtrack.track_name",
        "condition": "factstream.track_id = dimtrack.track_id",
    },
]

query_text = """
SELECT
    {% for param in parameters %}
    {{ param.cols }}{% if not loop.last %},{% endif %}
    {% endfor %}
FROM {{ parameters[0]['table'] }} AS {{ parameters[0]['alias'] }}
{% for param in parameters[1:] %}
LEFT JOIN {{ param['table'] }} AS {{ param['alias'] }}
    ON {{ param['condition'] }}
{% endfor %}
"""

template = Template(query_text)
query = template.render(parameters=parameters)
print(query)

# COMMAND ----------
display(spark.sql(query))



