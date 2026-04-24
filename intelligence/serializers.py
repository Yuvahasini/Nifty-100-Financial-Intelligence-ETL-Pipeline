"""
DRF Serializers for the Nifty 100 Intelligence API.
"""

from rest_framework import serializers
from .models import (
    DimCompany, FactProfitLoss, FactBalanceSheet,
    FactCashFlow, FactAnalysis, FactMlScore, FactProsCons,
)


class CompanyListSerializer(serializers.ModelSerializer):
    class Meta:
        model = DimCompany
        fields = ["symbol", "company_name", "sector", "website",
                  "roce_percentage", "roe_percentage"]


class CompanyDetailSerializer(serializers.ModelSerializer):
    class Meta:
        model = DimCompany
        fields = "__all__"


class ProfitLossSerializer(serializers.ModelSerializer):
    year_label  = serializers.CharField(source="year.year_label", read_only=True)
    fiscal_year = serializers.IntegerField(source="year.fiscal_year", read_only=True)

    class Meta:
        model = FactProfitLoss
        fields = ["year_label", "fiscal_year", "sales", "expenses",
                  "operating_profit", "opm_pct", "other_income", "interest",
                  "depreciation", "profit_before_tax", "tax_pct",
                  "net_profit", "eps", "dividend_payout_pct",
                  "net_profit_margin_pct", "expense_ratio_pct", "interest_coverage"]


class BalanceSheetSerializer(serializers.ModelSerializer):
    year_label  = serializers.CharField(source="year.year_label", read_only=True)
    fiscal_year = serializers.IntegerField(source="year.fiscal_year", read_only=True)

    class Meta:
        model = FactBalanceSheet
        fields = ["year_label", "fiscal_year", "equity_capital", "reserves",
                  "borrowings", "other_liabilities", "total_liabilities",
                  "fixed_assets", "cwip", "investments", "other_asset",
                  "total_assets", "debt_to_equity", "equity_ratio"]


class CashFlowSerializer(serializers.ModelSerializer):
    year_label  = serializers.CharField(source="year.year_label", read_only=True)
    fiscal_year = serializers.IntegerField(source="year.fiscal_year", read_only=True)

    class Meta:
        model = FactCashFlow
        fields = ["year_label", "fiscal_year", "operating_activity",
                  "investing_activity", "financing_activity",
                  "net_cash_flow", "free_cash_flow"]


class AnalysisSerializer(serializers.ModelSerializer):
    class Meta:
        model = FactAnalysis
        fields = ["period", "compounded_sales_growth_pct",
                  "compounded_profit_growth_pct", "stock_price_cagr_pct", "roe_pct"]


class MlScoreSerializer(serializers.ModelSerializer):
    class Meta:
        model = FactMlScore
        fields = ["computed_at", "overall_score", "profitability_score",
                  "growth_score", "leverage_score", "cashflow_score",
                  "dividend_score", "trend_score", "health_label"]


class ProsConsSerializer(serializers.ModelSerializer):
    class Meta:
        model = FactProsCons
        fields = ["is_pro", "text", "source", "created_at"]


class CompanyFullSerializer(serializers.ModelSerializer):
    """Single endpoint — full company snapshot."""
    profit_loss   = serializers.SerializerMethodField()
    balance_sheet = serializers.SerializerMethodField()
    cash_flow     = serializers.SerializerMethodField()
    analysis      = serializers.SerializerMethodField()
    ml_score      = serializers.SerializerMethodField()
    pros_cons     = serializers.SerializerMethodField()

    class Meta:
        model = DimCompany
        fields = ["symbol", "company_name", "sector", "website",
                  "about_company", "face_value", "book_value",
                  "roce_percentage", "roe_percentage",
                  "profit_loss", "balance_sheet", "cash_flow",
                  "analysis", "ml_score", "pros_cons"]

    def get_profit_loss(self, obj):
        qs = FactProfitLoss.objects.filter(symbol=obj).select_related("year").order_by("year__sort_order")
        return ProfitLossSerializer(qs, many=True).data

    def get_balance_sheet(self, obj):
        qs = FactBalanceSheet.objects.filter(symbol=obj).select_related("year").order_by("year__sort_order")
        return BalanceSheetSerializer(qs, many=True).data

    def get_cash_flow(self, obj):
        qs = FactCashFlow.objects.filter(symbol=obj).select_related("year").order_by("year__sort_order")
        return CashFlowSerializer(qs, many=True).data

    def get_analysis(self, obj):
        qs = FactAnalysis.objects.filter(symbol=obj)
        return AnalysisSerializer(qs, many=True).data

    def get_ml_score(self, obj):
        qs = FactMlScore.objects.filter(symbol=obj).order_by("-computed_at").first()
        return MlScoreSerializer(qs).data if qs else None

    def get_pros_cons(self, obj):
        qs = FactProsCons.objects.filter(symbol=obj)
        return ProsConsSerializer(qs, many=True).data