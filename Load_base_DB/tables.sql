CREATE TYPE BALANCE_T AS (
    num NUMERIC(12, 2)
);
COMMENT ON TYPE BALANCE_T IS 'Aggregate account and transaction related values such as  account balances, total commissions, etc.';
-- CDC_FLAG_T based on CHAR(1)
CREATE TYPE CDC_FLAG_T AS (
    flag CHAR(1)
);
COMMENT ON TYPE CDC_FLAG_T IS '“I”, “U” or “D” for insert, update or delete';
-- CDC_DSN_T based on NUM(12)
CREATE TYPE CDC_DSN_T AS (
    dsn NUMERIC(12)
);
COMMENT ON TYPE CDC_DSN_T IS 'Database Sequence Number, a monotonically increasing value';
-- IDENT_T based on NUM(11)
CREATE TYPE IDENT_T AS (
    ident NUMERIC(11)
);
COMMENT ON TYPE IDENT_T IS 'Numeric identifiers';
-- S_COUNT_T based on NUM(12)
CREATE TYPE S_COUNT_T AS (
    count NUMERIC(12)
);
COMMENT ON TYPE S_COUNT_T IS 'Aggregate count of shares';
-- S_PRICE_T based on SNUM(8,2)
CREATE TYPE S_PRICE_T AS (
    price NUMERIC(8, 2)
);
COMMENT ON TYPE S_PRICE_T IS 'Share prices';
-- S_QTY_T based on NUM(6)
CREATE TYPE S_QTY_T AS (
    qty NUMERIC(6)
);
COMMENT ON TYPE S_QTY_T IS 'Quantity of shares for an individual trade';

-- TRADE_T based on NUM(15)
CREATE TYPE TRADE_T AS (
    trade NUMERIC(15)
);
COMMENT ON TYPE TRADE_T IS 'Trade identifiers';
-- VALUE_T based on SNUM(10,2)
CREATE TYPE VALUE_T AS (
    value NUMERIC(10, 2)
);
COMMENT ON TYPE VALUE_T IS 'Non-aggregated transaction and security related values such as cost, dividend, etc.';

CREATE TABLE CashTransaction (
--    CDC_FLAG CDC_FLAG_T DEFAULT 'I' NOT NULL,
--    CDC_DSN CDC_DSN_T NOT NULL,
    CT_CA_ID IDENT_T NOT NULL,
    CT_DTS TIMESTAMPTZ NOT NULL,
    CT_AMT VALUE_T NOT NULL,
    CT_NAME VARCHAR(100) NOT NULL
);

CREATE TABLE DailyMarket (
--    CDC_FLAG CDC_FLAG_T DEFAULT 'I' NOT NULL,
--    CDC_DSN CDC_DSN_T NOT NULL,
    DM_DATE DATE NOT NULL,
	DM_S_SYMB CHAR(15) NOT NULL,
	DM_CLOSE S_PRICE_T NOT NULL,
	DM_HIGH S_PRICE_T NOT NULL,
	DM_LOW S_PRICE_T NOT NULL,
	DM_VOL S_PRICE_T NOT NULL
);

CREATE TABLE date (
	SK_DateID IDENT_T NOT null,
	DateValue CHAR(20) NOT null,
	DateDesc CHAR(20) NOT null,
	CalendarYearID NUMERIC(4) NOT null,
	CalendarYearDesc CHAR(20) NOT null,
	CalendarQtrID NUMERIC(5) NOT null,
	CalendarQtrDesc CHAR(20) NOT null,
	CalendarMonthID NUMERIC(6) NOT null,
	CalendarMonthDesc CHAR(20) NOT null,
	CalendarWeekID NUMERIC(6) NOT null,
	CalendarWeekDesc CHAR(20) NOT null,
	DayOfWeekNum NUMERIC(1) NOT null,
	DayOfWeekDesc CHAR(10) NOT null,
	FiscalYearID NUMERIC(4) NOT null,
	FiscalYearDesc CHAR(20) NOT null,
	FiscalQtrlID NUMERIC(5) NOT null,
	FiscalQtrDesc CHAR(20) NOT null,
	HolidayFlag BOOLEAN
);

CREATE TABLE finwire_cmp (
	PTS CHAR(15) NOT null,
	RecType CHAR(3) NOT null,
	CompanyName CHAR(60) NOT null,
	CIK CHAR(10) NOT null,
	Status CHAR(4) NOT null,
	IndustryID CHAR(2) NOT null,
	SPrating CHAR(4) NOT null,
	FoundingDate CHAR(8),
	AddrLine1 CHAR(80) NOT null,
	AddrLine2 CHAR(80),
	PostalCode CHAR(12) NOT null,
	City CHAR(25) NOT null,
	StateProvince CHAR(20) NOT null,
	Country CHAR(24),
	CEOname CHAR(46) NOT null,
	Description CHAR(150) NOT null
);

CREATE TABLE finwire_sec (
	PTS CHAR(15) NOT null,
	RecType CHAR(3) NOT null,
	Symbol CHAR(15) NOT null,
	Status CHAR(4) NOT null,
	Name CHAR(70) NOT null,
	ExID CHAR(6) NOT null,
	ShOut CHAR(13) NOT null,
	FirstTradeDate CHAR(8) NOT null,
	FirstTradeExchg CHAR(8) NOT null,
	Dividend CHAR(12) NOT null,
	CoNameOrCIK CHAR(60) NOT null
);

CREATE TABLE finwire_fin (
	PTS CHAR(15) NOT null,
	RecType CHAR(3) NOT null,
	Year CHAR(4) NOT null,
	Quarter CHAR(1) NOT null,
	QtrStartDate CHAR(8) NOT null,
	PostingDate CHAR(8) NOT null,
	Revenue CHAR(17) NOT null,
	Earnings CHAR(17) NOT null,
	EPS CHAR(12) NOT null,
	DilutedEPS CHAR(12) NOT null,
	Margin CHAR(12) NOT null,
	Inventory CHAR(17) NOT null,
	Assets CHAR(17) NOT null,
	Liabilities CHAR(17) NOT null,
	ShOut CHAR(13) NOT null,
	DilutedShOut CHAR(13) NOT null,
	CoNameOrClK CHAR(60) NOT null
);

CREATE TABLE HoldingHistory (
--    CDC_FLAG CDC_FLAG_T DEFAULT 'I' NOT NULL,
--    CDC_DSN CDC_DSN_T NOT NULL,
    HH_H_T_ID TRADE_T NOT NULL,
	HH_T_ID TRADE_T NOT NULL,
	HH_BEFORE_QTY S_QTY_T NOT NULL,
	HH_AFTER_QTY S_QTY_T NOT NULL
);

CREATE TABLE hr (
	EmployeeID IDENT_T not null,
	ManagerID IDENT_T NOT null,
	EmployeeFirstName CHAR(30) NOT null,
	EmployeeLastName CHAR(30) NOT null,
	EmployeeMI CHAR(1),
	EmployeeJobCode NUMERIC(3),
	EmployeeBranch CHAR(30),
	EmployeeOffice CHAR(10),
	EmployeePhone CHAR(14)
);

create table INDUSTRY (
	IN_ID CHAR(2) NOT NULL,
	IN_NAME CHAR(50) NOT NULL,
	IN_SC_ID CHAR(4) NOT NULL
);

create table PROSPECT(
	AgencyID CHAR(30) NOT NULL,
	LastName CHAR(30) NOT NULL,
	FirstName CHAR(30) NOT NULL,
	Middlelnitial CHAR(1),
	Gender CHAR(1),
	AddressLinel CHAR(80),
	AddressLine2 CHAR(80),
	PostalCode CHAR(12),
	City CHAR(25) NOT NULL,
	State CHAR(20) NOT NULL,
	Country CHAR(24),
	Phone CHAR(30),
	Income NUMERIC(9),
	NumberCars NUMERIC(2),
	NumberChildren NUMERIC(2),
	MaritalStatus CHAR(1),
	Age NUMERIC(3),
	CreditRating NUMERIC(4),
	OwnOrRentFlag CHAR(1),
	Employer CHAR(30),
	NumberCreditCards NUMERIC(2),
	NetWorth NUMERIC(12)
);

CREATE TABLE STATUSTYPE(
    ST_ID CHAR(4) NOT NULL,
    ST_NAME CHAR(10) NOT NULL
);

CREATE TABLE TaxRate(
    TX_ID CHAR(4) NOT NULL,
    TX_NAME CHAR(50) NOT NULL,
    TX_RATE NUMERIC(6,5) NOT NULL
);

CREATE TABLE Time(
    SK_TimeID IDENT_T NOT NULL,
    TimeValue CHAR(20) NOT NULL,
    HourID NUMERIC(2) NOT NULL,
    HourDesc CHAR(20) NOT NULL,
    MinuteID NUMERIC(2) NOT NULL,
    MinuteDesc CHAR(20) NOT NULL,
    SecondID NUMERIC(2) NOT NULL,
    SecondDesc CHAR(20) NOT NULL,
    MarketHoursFlag BOOLEAN,
    OfficeHoursFlag BOOLEAN
);

CREATE TABLE TradeHistory(
    TH_T_ID TRADE_T NOT NULL,
    TH_DTS TIMESTAMP NOT NULL,
    TH_ST_ID CHAR(4) NOT NULL
);

CREATE TABLE Trade(
--    CDC_FLAG CDC_FLAG_T DEFAULT 'I' NOT NULL,
--    CDC_DSN CDC_DSN_T NOT NULL,
    T_ID TRADE_T NOT NULL,
    T_DTS TIMESTAMP NOT NULL,
    T_ST_ID CHAR(4) NOT NULL,
    T_TT_ID CHAR(3) NOT NULL,
    T_IS_CASH BOOLEAN,
    T_S_SYMB CHAR(15) NOT NULL,
    T_QTY S_QTY_T,
    T_BID_PRICE S_PRICE_T,
    T_CA_ID IDENT_T NOT NULL,
    T_EXEC_NAME CHAR(49) NOT NULL,
    T_TRADE_PRICE S_PRICE_T,
    T_CHRG VALUE_T,
    T_COMM VALUE_T,
    T_TAX VALUE_T
);

CREATE TABLE TradeType(
    TT_ID CHAR(3) NOT NULL,
    TT_NAME CHAR(12) NOT NULL,
    TT_IS_SELL NUMERIC(1) NOT NULL,
    TT_IS_MRKT NUMERIC(1) NOT NULL
);

CREATE TABLE WatchHistory(
--    CDC_FLAG CDC_FLAG_T DEFAULT 'I' NOT NULL,
--    CDC_DSN CDC_DSN_T NOT NULL,
    W_C_ID IDENT_T NOT NULL,
    W_S_SYMB CHAR(15) NOT NULL,
    W_DTS timestamp NOT null,
    W_ACTION CHAR(4)
);