"""
App-level URL configuration for the intelligence app.
File location: intelligence/urls.py
"""

from django.urls import path
from drf_spectacular.views import (
    SpectacularAPIView,
    SpectacularSwaggerView,
    SpectacularRedocView,
)
from . import views

urlpatterns = [
    # ── Company endpoints ─────────────────────────────────────────────────
    path("companies/",                   views.CompanyListView.as_view(),   name="company-list"),
    path("companies/<str:symbol>/",      views.CompanyDetailView.as_view(), name="company-detail"),
    path("companies/<str:symbol>/full/", views.CompanyFullView.as_view(),   name="company-full"),

    # ── Financial data endpoints ──────────────────────────────────────────
    path("companies/<str:symbol>/profit-loss/",   views.ProfitLossView.as_view(),   name="profit-loss"),
    path("companies/<str:symbol>/balance-sheet/", views.BalanceSheetView.as_view(), name="balance-sheet"),
    path("companies/<str:symbol>/cash-flow/",     views.CashFlowView.as_view(),     name="cash-flow"),
    path("companies/<str:symbol>/analysis/",      views.AnalysisView.as_view(),     name="analysis"),
    path("companies/<str:symbol>/ml-score/",      views.MlScoreView.as_view(),      name="ml-score"),

    # ── Aggregation / comparison endpoints ───────────────────────────────
    path("leaderboard/",    views.leaderboard,       name="leaderboard"),
    path("sector-summary/", views.sector_summary,    name="sector-summary"),
    path("compare/",        views.compare_companies, name="compare"),

    # ── API docs ──────────────────────────────────────────────────────────
    path("schema/", SpectacularAPIView.as_view(), name="schema"),
    path("docs/",   SpectacularSwaggerView.as_view(url_name="schema"), name="swagger-ui"),
    path("redoc/",  SpectacularRedocView.as_view(url_name="schema"),   name="redoc"),
]