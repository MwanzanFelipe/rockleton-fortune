from django import template
import urllib

register = template.Library()

@register.filter(name='abs')
def abs_filter(value):
    return abs(value)

@register.filter(name='urlname')
def get_urlname(value):
    return urllib.quote(value).replace("/&amp;/g", "&")

