"""
Django models — read-only ORM layer over the PostgreSQL star-schema warehouse.
All tables are managed by the ETL pipeline; Django uses managed=False.
"""

from django.db import models


class DimCompany(models.Model):
    symbol        = models.CharField(max_length=20, primary_key=True)
    company_name  = models.TextField(null=True, blank=True)
    sector        = models.CharField(max_length=50, null=True, blank=True)
    company_logo  = models.TextField(null=True, blank=True)
    website       = models.TextField(null=True, blank=True)
    nse_profile   = models.TextField(null=True, blank=True)
    bse_profile   = models.TextField(null=True, blank=True)
    chart_link    = models.TextField(null=True, blank=True)
    about_company = models.TextField(null=True, blank=True)
    face_value    = models.DecimalField(max_digits=10, decimal_places=2, null=True)
    book_value    = models.DecimalField(max_digits=12, decimal_places=2, null=True)
    roce_percentage = models.DecimalField(max_digits=8, decimal_places=2, null=True)
    roe_percentage  = models.DecimalField(max_digits=8, decimal_places=2, null=True)

    class Meta:
        managed = False
        db_table = "dim_company"

    def __str__(self):
        return f"{self.symbol} — {self.company_name}"


class DimYear(models.Model):
    year_id      = models.IntegerField(primary_key=True)
    year_label   = models.CharField(max_length=20, unique=True)
    fiscal_year  = models.IntegerField(null=True)
    sort_order   = models.IntegerField(null=True)
    is_ttm       = models.BooleanField(default=False)
    is_half_year = models.BooleanField(default=False)

    class Meta:
        managed = False
        db_table = "dim_year"

    def __str__(self):
        return self.year_label


class DimSector(models.Model):
    sector_id   = models.AutoField(primary_key=True)
    sector_name = models.CharField(max_length=50, unique=True)

    class Meta:
        managed = False
        db_table = "dim_sector"

    def __str__(self):
        return self.sector_name


class DimHealthLabel(models.Model):
    label_id   = models.AutoField(primary_key=True)
    label_name = models.CharField(max_length=20, unique=True)
    min_score  = models.DecimalField(max_digits=5, decimal_places=2, null=True)
    max_score  = models.DecimalField(max_digits=5, decimal_places=2, null=True)
    color_hex  = models.CharField(max_length=7, null=True)

    class Meta:
        managed = False
        db_table = "dim_health_label"

    def __str__(self):
        return self.label_name


class FactProfitLoss(models.Model):
    id                   = models.AutoField(primary_key=True)
    symbol               = models.ForeignKey(DimCompany, db_column="symbol", on_delete=models.CASCADE)
    year                 = models.ForeignKey(DimYear, db_column="year_id", on_delete=models.CASCADE)
    sales                = models.DecimalField(max_digits=18, decimal_places=2, null=True)
    expenses             = models.DecimalField(max_digits=18, decimal_places=2, null=True)
    operating_profit     = models.DecimalField(max_digits=18, decimal_places=2, null=True)
    opm_pct              = models.DecimalField(max_digits=8,  decimal_places=2, null=True)
    other_income         = models.DecimalField(max_digits=18, decimal_places=2, null=True)
    interest             = models.DecimalField(max_digits=18, decimal_places=2, null=True)
    depreciation         = models.DecimalField(max_digits=18, decimal_places=2, null=True)
    profit_before_tax    = models.DecimalField(max_digits=18, decimal_places=2, null=True)
    tax_pct              = models.DecimalField(max_digits=8,  decimal_places=2, null=True)
    net_profit           = models.DecimalField(max_digits=18, decimal_places=2, null=True)
    eps                  = models.DecimalField(max_digits=10, decimal_places=2, null=True)
    dividend_payout_pct  = models.DecimalField(max_digits=8,  decimal_places=2, null=True)
    net_profit_margin_pct = models.DecimalField(max_digits=8, decimal_places=2, null=True)
    expense_ratio_pct    = models.DecimalField(max_digits=8,  decimal_places=2, null=True)
    interest_coverage    = models.DecimalField(max_digits=10, decimal_places=2, null=True)

    class Meta:
        managed = False
        db_table = "fact_profit_loss"
        unique_together = [("symbol", "year")]

    def __str__(self):
        return f"{self.symbol_id} {self.year_id}"


class FactBalanceSheet(models.Model):
    id                   = models.AutoField(primary_key=True)
    symbol               = models.ForeignKey(DimCompany, db_column="symbol", on_delete=models.CASCADE)
    year                 = models.ForeignKey(DimYear, db_column="year_id", on_delete=models.CASCADE)
    equity_capital       = models.DecimalField(max_digits=18, decimal_places=2, null=True)
    reserves             = models.DecimalField(max_digits=18, decimal_places=2, null=True)
    borrowings           = models.DecimalField(max_digits=18, decimal_places=2, null=True)
    other_liabilities    = models.DecimalField(max_digits=18, decimal_places=2, null=True)
    total_liabilities    = models.DecimalField(max_digits=18, decimal_places=2, null=True)
    fixed_assets         = models.DecimalField(max_digits=18, decimal_places=2, null=True)
    cwip                 = models.DecimalField(max_digits=18, decimal_places=2, null=True)
    investments          = models.DecimalField(max_digits=18, decimal_places=2, null=True)
    other_asset          = models.DecimalField(max_digits=18, decimal_places=2, null=True)
    total_assets         = models.DecimalField(max_digits=18, decimal_places=2, null=True)
    debt_to_equity       = models.DecimalField(max_digits=10, decimal_places=4, null=True)
    equity_ratio         = models.DecimalField(max_digits=10, decimal_places=4, null=True)

    class Meta:
        managed = False
        db_table = "fact_balance_sheet"
        unique_together = [("symbol", "year")]


class FactCashFlow(models.Model):
    id                   = models.AutoField(primary_key=True)
    symbol               = models.ForeignKey(DimCompany, db_column="symbol", on_delete=models.CASCADE)
    year                 = models.ForeignKey(DimYear, db_column="year_id", on_delete=models.CASCADE)
    operating_activity   = models.DecimalField(max_digits=18, decimal_places=2, null=True)
    investing_activity   = models.DecimalField(max_digits=18, decimal_places=2, null=True)
    financing_activity   = models.DecimalField(max_digits=18, decimal_places=2, null=True)
    net_cash_flow        = models.DecimalField(max_digits=18, decimal_places=2, null=True)
    free_cash_flow       = models.DecimalField(max_digits=18, decimal_places=2, null=True)

    class Meta:
        managed = False
        db_table = "fact_cash_flow"
        unique_together = [("symbol", "year")]


class FactAnalysis(models.Model):
    id                           = models.AutoField(primary_key=True)
    symbol                       = models.ForeignKey(DimCompany, db_column="symbol", on_delete=models.CASCADE)
    period                       = models.CharField(max_length=10)
    compounded_sales_growth_pct  = models.DecimalField(max_digits=8, decimal_places=2, null=True)
    compounded_profit_growth_pct = models.DecimalField(max_digits=8, decimal_places=2, null=True)
    stock_price_cagr_pct         = models.DecimalField(max_digits=8, decimal_places=2, null=True)
    roe_pct                      = models.DecimalField(max_digits=8, decimal_places=2, null=True)

    class Meta:
        managed = False
        db_table = "fact_analysis"
        unique_together = [("symbol", "period")]


class FactMlScore(models.Model):
    # symbol is the primary key — one row per company (latest score only)
    # computed_at is stored but not used as PK to avoid Django composite PK issues
    symbol              = models.OneToOneField(
                            DimCompany,
                            db_column="symbol",
                            on_delete=models.CASCADE,
                            primary_key=True,
                          )
    computed_at         = models.DateTimeField()
    overall_score       = models.DecimalField(max_digits=6, decimal_places=2, null=True)
    profitability_score = models.DecimalField(max_digits=6, decimal_places=2, null=True)
    growth_score        = models.DecimalField(max_digits=6, decimal_places=2, null=True)
    leverage_score      = models.DecimalField(max_digits=6, decimal_places=2, null=True)
    cashflow_score      = models.DecimalField(max_digits=6, decimal_places=2, null=True)
    dividend_score      = models.DecimalField(max_digits=6, decimal_places=2, null=True)
    trend_score         = models.DecimalField(max_digits=6, decimal_places=2, null=True)
    health_label        = models.CharField(max_length=20, null=True)

    class Meta:
        managed = False
        db_table = "fact_ml_scores"

    def __str__(self):
        return f"{self.symbol_id} — {self.overall_score} [{self.health_label}]"


class FactProsCons(models.Model):
    id         = models.AutoField(primary_key=True)
    symbol     = models.ForeignKey(DimCompany, db_column="symbol", on_delete=models.CASCADE)
    is_pro     = models.BooleanField(null=True)
    text       = models.TextField(null=True)
    source     = models.CharField(max_length=10, default="MANUAL")
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = False
        db_table = "fact_pros_cons"


class FactDocument(models.Model):
    id                = models.AutoField(primary_key=True)
    symbol            = models.ForeignKey(DimCompany, db_column="symbol", on_delete=models.CASCADE)
    year              = models.CharField(max_length=10, null=True)
    annual_report_url = models.TextField(null=True)

    class Meta:
        managed = False
        db_table = "fact_documents"