from django.contrib import admin

from .models import JunkMessage


class JunkMessageAdmin(admin.ModelAdmin):
    list_display = ['subscriber', 'error_message', 'offset', 'topic_key']


admin.site.register(JunkMessage, JunkMessageAdmin)
