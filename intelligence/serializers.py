from rest_framework import serializers
from .models import (
    DimCompany, FactProfitLoss, FactBalanceSheet,
    FactCashFlow, FactAnalysis, FactMlScore,
    FactProsCons, FactAnomalyFlag, FactPeerMapping,
)


class CompanyListSerializer(serializers.ModelSerializer):
    class Meta:
        model  = DimCompany
        fields = ["symbol", "company_name", "sector", "website",
                  "roce_percentage", "roe_percentage"]


class CompanyDetailSerializer(serializers.ModelSerializer):
    class Meta:
        model  = DimCompany
        fields = "__all__"


class ProfitLossSerializer(serializers.ModelSerializer):
    year_label  = serializers.CharField(source="year.year_label",    read_only=True)
    fiscal_year = serializers.IntegerField(source="year.fiscal_year", read_only=True)

    class Meta:
        model  = FactProfitLoss
        fields = [
            "year_label", "fiscal_year", "sales", "expenses",
            "operating_profit", "opm_pct", "other_income", "interest",
            "depreciation", "profit_before_tax", "tax_pct",
            "net_profit", "eps", "dividend_payout_pct",
            "net_profit_margin_pct", "expense_ratio_pct", "interest_coverage",
        ]


class BalanceSheetSerializer(serializers.ModelSerializer):
    year_label  = serializers.CharField(source="year.year_label",    read_only=True)
    fiscal_year = serializers.IntegerField(source="year.fiscal_year", read_only=True)

    class Meta:
        model  = FactBalanceSheet
        fields = [
            "year_label", "fiscal_year", "equity_capital", "reserves",
            "borrowings", "other_liabilities", "total_liabilities",
            "fixed_assets", "cwip", "investments", "other_asset",
            "total_assets", "debt_to_equity", "equity_ratio",
        ]


class CashFlowSerializer(serializers.ModelSerializer):
    year_label  = serializers.CharField(source="year.year_label",    read_only=True)
    fiscal_year = serializers.IntegerField(source="year.fiscal_year", read_only=True)

    class Meta:
        model  = FactCashFlow
        fields = [
            "year_label", "fiscal_year", "operating_activity",
            "investing_activity", "financing_activity",
            "net_cash_flow", "free_cash_flow",
        ]


class AnalysisSerializer(serializers.ModelSerializer):
    class Meta:
        model  = FactAnalysis
        fields = [
            "period", "compounded_sales_growth_pct",
            "compounded_profit_growth_pct", "stock_price_cagr_pct", "roe_pct",
        ]


class MlScoreSerializer(serializers.ModelSerializer):
    class Meta:
        model  = FactMlScore
        fields = [
            "computed_at", "overall_score", "profitability_score",
            "growth_score", "leverage_score", "cashflow_score",
            "dividend_score", "trend_score", "health_label",
        ]


class ProsConsSerializer(serializers.ModelSerializer):
    class Meta:
        model  = FactProsCons
        fields = ["is_pro", "text"]


class AnomalyFlagSerializer(serializers.ModelSerializer):
    class Meta:
        model  = FactAnomalyFlag
        fields = [
            "fiscal_year", "metric", "z_score",
            "direction", "method", "flagged_at",
        ]


class PeerMappingSerializer(serializers.ModelSerializer):
    peer_symbol = serializers.SerializerMethodField()
    peer_name   = serializers.SerializerMethodField()
    peer_sector = serializers.SerializerMethodField()

    class Meta:
        model  = FactPeerMapping
        fields = ["peer_rank", "peer_symbol", "peer_name", "peer_sector", "similarity_score"]

    def get_peer_symbol(self, obj):
        try:
            return obj.peer_symbol_id
        except Exception:
            return None

    def get_peer_name(self, obj):
        try:
            return obj.peer_symbol.company_name
        except Exception:
            return None

    def get_peer_sector(self, obj):
        try:
            return obj.peer_symbol.sector
        except Exception:
            return None


class CompanyFullSerializer(serializers.ModelSerializer):
    latest_score = serializers.SerializerMethodField()
    pros         = serializers.SerializerMethodField()
    cons         = serializers.SerializerMethodField()
    peers        = serializers.SerializerMethodField()

    class Meta:
        model  = DimCompany
        fields = [
            "symbol", "company_name", "sector", "website",
            "roce_percentage", "roe_percentage",
            "latest_score", "pros", "cons", "peers",
        ]

    def get_latest_score(self, obj):
        try:
            score = FactMlScore.objects.filter(
                symbol=obj).order_by("-computed_at").first()
            return MlScoreSerializer(score).data if score else None
        except Exception:
            return None

    def get_pros(self, obj):
        try:
            qs = FactProsCons.objects.filter(
                symbol=obj, is_pro=True)[:5]
            return ProsConsSerializer(qs, many=True).data
        except Exception:
            return []

    def get_cons(self, obj):
        try:
            qs = FactProsCons.objects.filter(
                symbol=obj, is_pro=False)[:5]
            return ProsConsSerializer(qs, many=True).data
        except Exception:
            return []

    def get_peers(self, obj):
        try:
            qs = FactPeerMapping.objects.filter(
                symbol=obj).select_related("peer_symbol").order_by("peer_rank")
            return PeerMappingSerializer(qs, many=True).data
        except Exception:
            return []