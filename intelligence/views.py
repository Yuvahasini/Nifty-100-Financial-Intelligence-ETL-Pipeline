"""
Django REST Framework views for the Nifty 100 Intelligence API.
"""

from rest_framework import generics, filters
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework.pagination import PageNumberPagination
from drf_spectacular.utils import extend_schema, OpenApiParameter
from drf_spectacular.types import OpenApiTypes

from .models import (
    DimCompany, FactProfitLoss, FactBalanceSheet,
    FactCashFlow, FactAnalysis, FactMlScore,
    FactAnomalyFlag, FactPeerMapping, FactProsCons,
)
from .serializers import (
    CompanyListSerializer, CompanyDetailSerializer,
    CompanyFullSerializer, ProfitLossSerializer,
    BalanceSheetSerializer, CashFlowSerializer,
    AnalysisSerializer, MlScoreSerializer,
    AnomalyFlagSerializer, PeerMappingSerializer,
)


class StandardPagination(PageNumberPagination):
    page_size             = 20
    page_size_query_param = "page_size"
    max_page_size         = 100


# ---------------------------------------------------------------------------
# Company endpoints
# ---------------------------------------------------------------------------

class CompanyListView(generics.ListAPIView):
    serializer_class = CompanyListSerializer
    pagination_class = StandardPagination
    filter_backends  = [filters.SearchFilter, filters.OrderingFilter]
    search_fields    = ["symbol", "company_name"]
    ordering_fields  = ["symbol", "company_name", "sector"]
    ordering         = ["symbol"]

    def get_queryset(self):
        qs = DimCompany.objects.all()
        sector = self.request.query_params.get("sector")
        if sector:
            qs = qs.filter(sector__iexact=sector)
        health_label = self.request.query_params.get("health_label")
        if health_label:
            syms = (FactMlScore.objects
                    .filter(health_label__iexact=health_label)
                    .values_list("symbol_id", flat=True).distinct())
            qs = qs.filter(symbol__in=syms)
        return qs


class CompanyDetailView(generics.RetrieveAPIView):
    serializer_class = CompanyDetailSerializer
    queryset         = DimCompany.objects.all()
    lookup_field     = "symbol"


class CompanyFullView(generics.RetrieveAPIView):
    serializer_class = CompanyFullSerializer
    queryset         = DimCompany.objects.all()
    lookup_field     = "symbol"


# ---------------------------------------------------------------------------
# Financial data endpoints
# ---------------------------------------------------------------------------

class ProfitLossView(generics.ListAPIView):
    serializer_class = ProfitLossSerializer

    def get_queryset(self):
        return (FactProfitLoss.objects
                .filter(symbol_id=self.kwargs["symbol"])
                .select_related("year")
                .order_by("year__sort_order"))


class BalanceSheetView(generics.ListAPIView):
    serializer_class = BalanceSheetSerializer

    def get_queryset(self):
        return (FactBalanceSheet.objects
                .filter(symbol_id=self.kwargs["symbol"])
                .select_related("year")
                .order_by("year__sort_order"))


class CashFlowView(generics.ListAPIView):
    serializer_class = CashFlowSerializer

    def get_queryset(self):
        return (FactCashFlow.objects
                .filter(symbol_id=self.kwargs["symbol"])
                .select_related("year")
                .order_by("year__sort_order"))


class AnalysisView(generics.ListAPIView):
    serializer_class = AnalysisSerializer

    def get_queryset(self):
        return (FactAnalysis.objects
                .filter(symbol_id=self.kwargs["symbol"])
                .order_by("period"))


class MlScoreView(generics.ListAPIView):
    serializer_class = MlScoreSerializer

    def get_queryset(self):
        return (FactMlScore.objects
                .filter(symbol_id=self.kwargs["symbol"])
                .order_by("-computed_at")[:5])


class AnomalyFlagView(generics.ListAPIView):
    serializer_class = AnomalyFlagSerializer

    def get_queryset(self):
        return (FactAnomalyFlag.objects
                .filter(symbol_id=self.kwargs["symbol"])
                .order_by("-flagged_at"))


class PeerView(generics.ListAPIView):
    serializer_class = PeerMappingSerializer

    def get_queryset(self):
        return (FactPeerMapping.objects
                .filter(symbol_id=self.kwargs["symbol"])
                .select_related("peer_symbol")
                .order_by("peer_rank"))


# ---------------------------------------------------------------------------
# Aggregation / comparison endpoints
# ---------------------------------------------------------------------------

@api_view(["GET"])
def leaderboard(request):
    n      = int(request.query_params.get("n", 20))
    sector = request.query_params.get("sector")

    qs = FactMlScore.objects.select_related("symbol").order_by("-overall_score")
    if sector:
        qs = qs.filter(symbol__sector__iexact=sector)

    results, seen = [], set()
    for row in qs:
        sym = row.symbol_id
        if sym in seen:
            continue
        seen.add(sym)
        results.append({
            "rank":          len(results) + 1,
            "symbol":        sym,
            "company_name":  row.symbol.company_name,
            "sector":        row.symbol.sector,
            "overall_score": float(row.overall_score or 0),
            "health_label":  row.health_label,
            "profitability": float(row.profitability_score or 0),
            "growth":        float(row.growth_score or 0),
            "leverage":      float(row.leverage_score or 0),
            "cashflow":      float(row.cashflow_score or 0),
            "dividend":      float(row.dividend_score or 0),
            "trend":         float(row.trend_score or 0),
        })
        if len(results) >= n:
            break

    return Response(results)


@api_view(["GET"])
def sector_summary(request):
    from django.db.models import Avg, Count
    data = (FactMlScore.objects
            .select_related("symbol")
            .values("symbol__sector")
            .annotate(
                avg_overall=Avg("overall_score"),
                avg_profitability=Avg("profitability_score"),
                avg_growth=Avg("growth_score"),
                avg_leverage=Avg("leverage_score"),
                avg_cashflow=Avg("cashflow_score"),
                avg_dividend=Avg("dividend_score"),
                avg_trend=Avg("trend_score"),
                company_count=Count("symbol", distinct=True),
            )
            .order_by("-avg_overall"))
    return Response(list(data))


@api_view(["GET"])
def compare_companies(request):
    symbols = request.query_params.getlist("symbol")
    if not symbols:
        return Response({"error": "Pass at least one ?symbol= parameter"}, status=400)

    results = []
    for sym in symbols:
        try:
            company = DimCompany.objects.get(symbol=sym.upper())
            score   = FactMlScore.objects.filter(symbol=company).order_by("-computed_at").first()
            results.append({
                "symbol":              company.symbol,
                "company_name":        company.company_name,
                "sector":              company.sector,
                "roce":                float(company.roce_percentage or 0),
                "roe":                 float(company.roe_percentage or 0),
                "overall_score":       float(score.overall_score or 0)       if score else None,
                "profitability_score": float(score.profitability_score or 0) if score else None,
                "growth_score":        float(score.growth_score or 0)        if score else None,
                "leverage_score":      float(score.leverage_score or 0)      if score else None,
                "cashflow_score":      float(score.cashflow_score or 0)      if score else None,
                "dividend_score":      float(score.dividend_score or 0)      if score else None,
                "trend_score":         float(score.trend_score or 0)         if score else None,
                "health_label":        score.health_label                    if score else None,
            })
        except DimCompany.DoesNotExist:
            results.append({"symbol": sym, "error": "Not found"})

    return Response(results)


@api_view(["GET"])
def all_anomalies(request):
    qs = FactAnomalyFlag.objects.select_related("symbol").order_by("-flagged_at")

    method = request.query_params.get("method")
    if method:
        qs = qs.filter(method=method)

    metric = request.query_params.get("metric")
    if metric:
        qs = qs.filter(metric=metric)

    data = [
        {
            "symbol":      row.symbol_id,
            "fiscal_year": row.fiscal_year,
            "metric":      row.metric,
            "z_score":     float(row.z_score) if row.z_score else None,
            "direction":   row.direction,
            "method":      row.method,
            "flagged_at":  str(row.flagged_at),
        }
        for row in qs[:200]
    ]
    return Response(data)