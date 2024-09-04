from django.urls import path

from . import views

urlpatterns = [
    path("exchanges/", views.list_exchanges, name="get_exchanges"),  # type: ignore
    path("price/", views.fetch, name="fetch_pairs"),  # type: ignore
]
