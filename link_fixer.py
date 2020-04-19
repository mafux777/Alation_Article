# -- This module can be used to find any links that are broken
# It is meant to be used on the command line
# directly on the server

regexp='<a\s+data-oid="([0-9]+)"\s+data-otype="([a-z]+)"\s+href="([a-z0-9\/]+)">(\w+)'