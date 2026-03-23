-- =============================================================================
-- OLTP SEED DATA — Belgian Sanitary & Building Materials Distributor
-- Company: BelSani NV
-- Scale: 5 locations, 50 products, 10 suppliers, 40 customers, ~3 months history
-- Period: 2024-10-01 → 2024-12-31
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 0. SCHEMA CREATION
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS locations (
    location_id      SERIAL PRIMARY KEY,
    location_code    VARCHAR(10)  NOT NULL UNIQUE,
    location_name    VARCHAR(100) NOT NULL,
    location_type    VARCHAR(20)  NOT NULL CHECK (location_type IN ('sanicenter','regional_warehouse')),
    address          VARCHAR(200),
    city             VARCHAR(100),
    postal_code      VARCHAR(10),
    region           VARCHAR(50),
    storage_capacity_m3 NUMERIC(10,2),
    is_active        BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS suppliers (
    supplier_id         SERIAL PRIMARY KEY,
    supplier_code       VARCHAR(10)  NOT NULL UNIQUE,
    supplier_name       VARCHAR(100) NOT NULL,
    contact_name        VARCHAR(100),
    email               VARCHAR(150),
    country             VARCHAR(50),
    avg_lead_time_days  NUMERIC(5,1),
    reliability_score   NUMERIC(3,2) CHECK (reliability_score BETWEEN 0 AND 1),
    is_active           BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS products (
    product_id       SERIAL PRIMARY KEY,
    sku              VARCHAR(20)  NOT NULL UNIQUE,
    product_name     VARCHAR(150) NOT NULL,
    category         VARCHAR(50),
    subcategory      VARCHAR(50),
    brand            VARCHAR(50),
    unit_of_measure  VARCHAR(20),
    unit_weight_kg   NUMERIC(8,3),
    unit_price_eur   NUMERIC(10,2),
    lead_time_days   INTEGER,
    min_order_qty    NUMERIC(8,2),
    is_active        BOOLEAN DEFAULT TRUE,
    created_at       TIMESTAMP DEFAULT NOW(),
    updated_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS customers (
    customer_id     SERIAL PRIMARY KEY,
    customer_code   VARCHAR(10)  NOT NULL UNIQUE,
    customer_name   VARCHAR(150) NOT NULL,
    customer_type   VARCHAR(30)  CHECK (customer_type IN ('plumber','contractor','architect','retailer','individual')),
    address         VARCHAR(200),
    city            VARCHAR(100),
    postal_code     VARCHAR(10),
    region          VARCHAR(50),
    is_professional BOOLEAN DEFAULT TRUE,
    is_active       BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS product_suppliers (
    id                      SERIAL PRIMARY KEY,
    product_id              INTEGER NOT NULL REFERENCES products(product_id),
    supplier_id             INTEGER NOT NULL REFERENCES suppliers(supplier_id),
    supplier_unit_cost_eur  NUMERIC(10,2),
    supplier_lead_time_days INTEGER,
    is_preferred            BOOLEAN DEFAULT FALSE,
    UNIQUE (product_id, supplier_id)
);

CREATE TABLE IF NOT EXISTS inventory (
    inventory_id    SERIAL PRIMARY KEY,
    product_id      INTEGER NOT NULL REFERENCES products(product_id),
    location_id     INTEGER NOT NULL REFERENCES locations(location_id),
    qty_on_hand     NUMERIC(10,2) DEFAULT 0,
    qty_reserved    NUMERIC(10,2) DEFAULT 0,
    min_stock_level NUMERIC(10,2) DEFAULT 0,
    reorder_point   NUMERIC(10,2) DEFAULT 0,
    max_stock_level NUMERIC(10,2) DEFAULT 0,
    last_updated    TIMESTAMP DEFAULT NOW(),
    UNIQUE (product_id, location_id)
);

CREATE TABLE IF NOT EXISTS sales_orders (
    order_id         SERIAL PRIMARY KEY,
    customer_id      INTEGER NOT NULL REFERENCES customers(customer_id),
    location_id      INTEGER NOT NULL REFERENCES locations(location_id),
    order_ts         TIMESTAMP NOT NULL,
    status           VARCHAR(20) CHECK (status IN ('pending','fulfilled','partial','cancelled')),
    source           VARCHAR(20) CHECK (source IN ('counter','phone','online','erp_import')),
    total_amount_eur NUMERIC(12,2)
);

CREATE TABLE IF NOT EXISTS sales_order_lines (
    line_id         SERIAL PRIMARY KEY,
    order_id        INTEGER NOT NULL REFERENCES sales_orders(order_id),
    product_id      INTEGER NOT NULL REFERENCES products(product_id),
    qty_ordered     NUMERIC(10,2),
    qty_fulfilled   NUMERIC(10,2),
    unit_price_eur  NUMERIC(10,2),
    line_total_eur  NUMERIC(12,2)
);

CREATE TABLE IF NOT EXISTS purchase_orders (
    po_id              SERIAL PRIMARY KEY,
    supplier_id        INTEGER NOT NULL REFERENCES suppliers(supplier_id),
    location_id        INTEGER NOT NULL REFERENCES locations(location_id),
    created_at         TIMESTAMP NOT NULL,
    expected_delivery  DATE,
    actual_delivery    DATE,
    status             VARCHAR(20) CHECK (status IN ('draft','sent','confirmed','received','partial','cancelled')),
    total_cost_eur     NUMERIC(12,2)
);

CREATE TABLE IF NOT EXISTS purchase_order_lines (
    line_id        SERIAL PRIMARY KEY,
    po_id          INTEGER NOT NULL REFERENCES purchase_orders(po_id),
    product_id     INTEGER NOT NULL REFERENCES products(product_id),
    qty_ordered    NUMERIC(10,2),
    qty_received   NUMERIC(10,2),
    unit_cost_eur  NUMERIC(10,2),
    line_total_eur NUMERIC(12,2)
);

CREATE TABLE IF NOT EXISTS inventory_movements (
    movement_id    SERIAL PRIMARY KEY,
    product_id     INTEGER NOT NULL REFERENCES products(product_id),
    location_id    INTEGER NOT NULL REFERENCES locations(location_id),
    movement_type  VARCHAR(20) CHECK (movement_type IN ('sale','po_receipt','adjustment','transfer','initial_stock')),
    qty_delta      NUMERIC(10,2) NOT NULL,
    ref_order_id   INTEGER,
    ref_order_type VARCHAR(20),
    movement_ts    TIMESTAMP NOT NULL,
    notes          TEXT
);

-- =============================================================================
-- 1. LOCATIONS — 3 Sanicenters + 2 Regional Warehouses
-- =============================================================================

INSERT INTO locations (location_code, location_name, location_type, address, city, postal_code, region, storage_capacity_m3, is_active) VALUES
('LOC-BRU',  'Sanicenter Brussel',        'sanicenter',         'Chaussée de Louvain 412',   'Brussel',    '1030', 'Brussels',         850.00,  TRUE),
('LOC-ANT',  'Sanicenter Antwerpen',       'sanicenter',         'Turnhoutsebaan 298',        'Antwerpen',  '2140', 'Antwerp',          920.00,  TRUE),
('LOC-GNT',  'Sanicenter Gent',            'sanicenter',         'Brusselsesteenweg 558',     'Gent',       '9050', 'East Flanders',    780.00,  TRUE),
('WH-LGE',   'Regionaal Warehouse Luik',   'regional_warehouse', 'Rue de Herve 890',          'Liège',      '4000', 'Liège',            4200.00, TRUE),
('WH-CHA',   'Regionaal Warehouse Charleroi','regional_warehouse','Avenue de Waterloo 1240',  'Charleroi',  '6000', 'Hainaut',          3800.00, TRUE);

-- =============================================================================
-- 2. SUPPLIERS — 10 European suppliers
-- =============================================================================

INSERT INTO suppliers (supplier_code, supplier_name, contact_name, email, country, avg_lead_time_days, reliability_score, is_active) VALUES
('SUP-001', 'Grohe AG',                  'Klaus Bauer',       'k.bauer@grohe.com',          'Germany',     5.0,  0.96, TRUE),
('SUP-002', 'Roca Sanitario SA',         'Carlos Mendez',     'c.mendez@roca.com',          'Spain',      12.0,  0.88, TRUE),
('SUP-003', 'Geberit AG',               'Hans Müller',       'h.mueller@geberit.com',       'Switzerland',  7.0,  0.97, TRUE),
('SUP-004', 'Duravit AG',               'Sophie Klein',      's.klein@duravit.com',         'Germany',     8.0,  0.92, TRUE),
('SUP-005', 'Watts Water Technologies', 'Jean Dupont',       'j.dupont@watts.com',          'France',      6.0,  0.90, TRUE),
('SUP-006', 'Viega GmbH',              'Markus Vogel',      'm.vogel@viega.com',            'Germany',     5.0,  0.95, TRUE),
('SUP-007', 'Hep2O (Wavin)',           'Peter Smits',       'p.smits@wavin.com',            'Netherlands', 4.0,  0.93, TRUE),
('SUP-008', 'Caleffi SpA',             'Marco Rossi',       'm.rossi@caleffi.com',          'Italy',      10.0,  0.87, TRUE),
('SUP-009', 'Sanitec Europe',          'Luc Janssen',       'l.janssen@sanitec.com',        'Belgium',     3.0,  0.94, TRUE),
('SUP-010', 'Vaillant Group',          'Thomas Richter',    't.richter@vaillant.com',       'Germany',    14.0,  0.85, TRUE);

-- =============================================================================
-- 3. PRODUCTS — 50 sanitary & building materials SKUs
-- =============================================================================

INSERT INTO products (sku, product_name, category, subcategory, brand, unit_of_measure, unit_weight_kg, unit_price_eur, lead_time_days, min_order_qty, is_active) VALUES
-- Sanitair - Toiletten
('SKU-00001', 'Hangtoilet Rimless wit',                  'Sanitair',   'Toiletten',          'Duravit',  'stuk',  22.50,  389.00,  8, 1.00, TRUE),
('SKU-00002', 'Staand toilet compact 48cm',              'Sanitair',   'Toiletten',          'Roca',     'stuk',  18.20,  219.00, 12, 1.00, TRUE),
('SKU-00003', 'Inbouwreservoir 6/3L dual flush',         'Sanitair',   'Toiletten',          'Geberit',  'stuk',   4.80,  145.00,  7, 1.00, TRUE),
('SKU-00004', 'Bedieningsplaat mat zwart',               'Sanitair',   'Toiletten',          'Geberit',  'stuk',   0.95,   89.00,  7, 1.00, TRUE),
('SKU-00005', 'WC-zitting softclose universeel',        'Sanitair',   'Toiletten',          'Grohe',    'stuk',   1.20,   45.00,  5, 2.00, TRUE),
-- Sanitair - Wasbakken
('SKU-00006', 'Opzetwastafel rond 40cm wit',             'Sanitair',   'Wasbakken',          'Duravit',  'stuk',   5.40,  159.00,  8, 1.00, TRUE),
('SKU-00007', 'Inbouwwastafel 60x46cm wit',              'Sanitair',   'Wasbakken',          'Roca',     'stuk',   9.10,  129.00, 12, 1.00, TRUE),
('SKU-00008', 'Fontein 38x28cm keramisch',               'Sanitair',   'Wasbakken',          'Sanitec',  'stuk',   2.80,   79.00,  3, 1.00, TRUE),
('SKU-00009', 'Wastafelkraan eengreeps chroom',          'Sanitair',   'Kranen',             'Grohe',    'stuk',   0.75,   98.00,  5, 2.00, TRUE),
('SKU-00010', 'Fonteinkraan laag model chroom',          'Sanitair',   'Kranen',             'Grohe',    'stuk',   0.45,   65.00,  5, 2.00, TRUE),
-- Sanitair - Douche
('SKU-00011', 'Douchebak 90x90cm acryl wit',             'Sanitair',   'Douche',             'Roca',     'stuk',  12.00,  189.00, 12, 1.00, TRUE),
('SKU-00012', 'Inloopdouche 100x200cm helder glas',     'Sanitair',   'Douche',             'Grohe',    'stuk',  18.50,  549.00,  5, 1.00, TRUE),
('SKU-00013', 'Douchekraan thermostatisch inbouw',       'Sanitair',   'Kranen',             'Grohe',    'stuk',   1.10,  245.00,  5, 1.00, TRUE),
('SKU-00014', 'Regendouche plafond 30cm chroom',         'Sanitair',   'Douche',             'Grohe',    'stuk',   0.95,   89.00,  5, 2.00, TRUE),
('SKU-00015', 'Doucheslang 150cm antiknik',              'Sanitair',   'Douche',             'Grohe',    'stuk',   0.18,   18.50,  5, 5.00, TRUE),
-- Verwarming
('SKU-00016', 'Paneelradiator 600x1000mm type 22',       'Verwarming', 'Radiatoren',         'Vaillant',  'stuk',  22.00,  189.00, 14, 1.00, TRUE),
('SKU-00017', 'Paneelradiator 600x600mm type 11',        'Verwarming', 'Radiatoren',         'Vaillant',  'stuk',  11.50,   99.00, 14, 1.00, TRUE),
('SKU-00018', 'Thermostatische radiatorkraan 1/2"',      'Verwarming', 'Radiatorkranen',     'Caleffi',  'stuk',   0.22,   18.50, 10, 5.00, TRUE),
('SKU-00019', 'Afsluiter bolvormig 3/4" PN25',           'Verwarming', 'Afsluiters',         'Watts',    'stuk',   0.30,   12.00,  6, 10.00,TRUE),
('SKU-00020', 'Expansievat 18L CV rood',                 'Verwarming', 'Expansievaten',      'Caleffi',  'stuk',   2.50,   49.00, 10, 1.00, TRUE),
-- Leidingen & Fittingen
('SKU-00021', 'Koperleiding 15mm per meter',             'Leidingen',  'Koper',              'Viega',    'meter',  0.10,    5.80,  5, 10.00,TRUE),
('SKU-00022', 'Koperleiding 22mm per meter',             'Leidingen',  'Koper',              'Viega',    'meter',  0.19,    9.20,  5, 10.00,TRUE),
('SKU-00023', 'Koperleiding 28mm per meter',             'Leidingen',  'Koper',              'Viega',    'meter',  0.28,   13.50,  5, 10.00,TRUE),
('SKU-00024', 'Propress fitting knie 15mm',              'Leidingen',  'Persfittingen',      'Viega',    'stuk',   0.04,    3.20,  5, 20.00,TRUE),
('SKU-00025', 'Propress fitting knie 22mm',              'Leidingen',  'Persfittingen',      'Viega',    'stuk',   0.08,    5.90,  5, 20.00,TRUE),
('SKU-00026', 'Propress T-stuk 15mm',                    'Leidingen',  'Persfittingen',      'Viega',    'stuk',   0.06,    4.50,  5, 20.00,TRUE),
('SKU-00027', 'Propress T-stuk 22mm',                    'Leidingen',  'Persfittingen',      'Viega',    'stuk',   0.12,    7.80,  5, 20.00,TRUE),
('SKU-00028', 'PE-Xc buis 16mm per meter',              'Leidingen',  'Kunststof',          'Hep2O',    'meter',  0.04,    1.20,  4, 50.00,TRUE),
('SKU-00029', 'PE-Xc buis 20mm per meter',              'Leidingen',  'Kunststof',          'Hep2O',    'meter',  0.06,    1.75,  4, 50.00,TRUE),
('SKU-00030', 'Meerlagenleiding 16x2mm per meter',      'Leidingen',  'Meerlagenleiding',   'Hep2O',    'meter',  0.05,    2.10,  4, 50.00,TRUE),
-- Sanitaire hulpstukken
('SKU-00031', 'Sifon wastafel 1"1/4 chroom',            'Hulpstukken','Sifons',              'Viega',    'stuk',   0.25,   14.50,  5, 5.00, TRUE),
('SKU-00032', 'Sifon douche flach 90mm',                 'Hulpstukken','Sifons',              'Viega',    'stuk',   0.35,   22.00,  5, 5.00, TRUE),
('SKU-00033', 'Flexibel aansluitslang 30cm 3/8"',        'Hulpstukken','Aansluitingen',      'Watts',    'stuk',   0.06,    4.20,  6, 10.00,TRUE),
('SKU-00034', 'Flexibel aansluitslang 50cm 3/8"',        'Hulpstukken','Aansluitingen',      'Watts',    'stuk',   0.08,    4.80,  6, 10.00,TRUE),
('SKU-00035', 'Stopkraan recht 1/2" chroom',             'Hulpstukken','Stopkranen',         'Watts',    'stuk',   0.14,    9.80,  6, 10.00,TRUE),
-- Isolatie & Dichtingen
('SKU-00036', 'Leidingschuim isolatie 15mm/1m',         'Isolatie',   'Buisisolatie',       'Sanitec',  'stuk',   0.08,    2.40,  3, 20.00,TRUE),
('SKU-00037', 'Leidingschuim isolatie 22mm/1m',         'Isolatie',   'Buisisolatie',       'Sanitec',  'stuk',   0.10,    3.10,  3, 20.00,TRUE),
('SKU-00038', 'PTFE tape 12mm x 12m',                   'Hulpstukken','Dichtingen',         'Sanitec',  'stuk',   0.02,    1.20,  3, 50.00,TRUE),
('SKU-00039', 'Hennep dichttouw 50gr',                   'Hulpstukken','Dichtingen',         'Sanitec',  'stuk',   0.06,    3.80,  3, 20.00,TRUE),
-- Warmwatertoestellen
('SKU-00040', 'Boiler elektrisch 80L staand',            'Verwarming', 'Warmwatertoestellen','Vaillant',  'stuk',  32.00,  389.00, 14, 1.00, TRUE),
('SKU-00041', 'Boiler elektrisch 150L staand',           'Verwarming', 'Warmwatertoestellen','Vaillant',  'stuk',  48.00,  549.00, 14, 1.00, TRUE),
('SKU-00042', 'Doorstroomtoestel gas 24kW',              'Verwarming', 'Warmwatertoestellen','Vaillant',  'stuk',  26.00,  699.00, 14, 1.00, TRUE),
-- Kranen overig
('SKU-00043', 'Keukenkraan draaibaar uitloop chroom',   'Sanitair',   'Kranen',             'Grohe',    'stuk',   0.82,  129.00,  5, 1.00, TRUE),
('SKU-00044', 'Badkraan vrijstaand chroom',              'Sanitair',   'Kranen',             'Grohe',    'stuk',   1.45,  289.00,  5, 1.00, TRUE),
('SKU-00045', 'Doucheset 3-functies handdouche',        'Sanitair',   'Douche',             'Grohe',    'stuk',   0.55,   79.00,  5, 2.00, TRUE),
-- Afvoer
('SKU-00046', 'PVC afvoerbuis 40mm per meter',          'Leidingen',  'PVC Afvoer',         'Hep2O',    'meter',  0.35,    3.90,  4, 10.00,TRUE),
('SKU-00047', 'PVC afvoerbuis 50mm per meter',          'Leidingen',  'PVC Afvoer',         'Hep2O',    'meter',  0.48,    5.20,  4, 10.00,TRUE),
('SKU-00048', 'PVC bocht 45° 40mm',                     'Leidingen',  'PVC Afvoer',         'Hep2O',    'stuk',   0.09,    2.10,  4, 20.00,TRUE),
('SKU-00049', 'PVC T-stuk 50mm',                        'Leidingen',  'PVC Afvoer',         'Hep2O',    'stuk',   0.18,    3.80,  4, 20.00,TRUE),
('SKU-00050', 'Siliconekit sanitair wit 310ml',         'Hulpstukken','Dichtingen',         'Sanitec',  'stuk',   0.38,    6.50,  3, 12.00,TRUE);

-- =============================================================================
-- 4. CUSTOMERS — 40 Belgian professional customers
-- =============================================================================

INSERT INTO customers (customer_code, customer_name, customer_type, address, city, postal_code, region, is_professional, is_active) VALUES
('CUST-001', 'Loodgieterij Van den Berg BVBA',  'plumber',      'Koningsstraat 14',         'Brussel',     '1000', 'Brussels',       TRUE,  TRUE),
('CUST-002', 'Sanitair Service Peeters',         'plumber',      'Turnhoutsebaan 87',        'Antwerpen',   '2140', 'Antwerp',        TRUE,  TRUE),
('CUST-003', 'Installatiebedrijf De Smet NV',   'contractor',   'Heuvelstraat 34',          'Gent',        '9000', 'East Flanders',  TRUE,  TRUE),
('CUST-004', 'Bouwgroep Martens & Zonen',       'contractor',   'Industrielaan 120',        'Leuven',      '3000', 'Flemish Brabant',TRUE,  TRUE),
('CUST-005', 'Technifluid SPRL',                'plumber',      'Rue de Namur 45',          'Namur',       '5000', 'Namur',          TRUE,  TRUE),
('CUST-006', 'Robinet & Fils SA',               'plumber',      'Avenue de la Gare 78',     'Liège',       '4000', 'Liège',          TRUE,  TRUE),
('CUST-007', 'BV Installaties Claes',           'contractor',   'Bredabaan 233',            'Antwerpen',   '2930', 'Antwerp',        TRUE,  TRUE),
('CUST-008', 'Architect Bureau Vermeersch',     'architect',    'Coupure Links 519',        'Gent',        '9000', 'East Flanders',  TRUE,  TRUE),
('CUST-009', 'Rénovation Leblanc BVBA',         'contractor',   'Rue des Mineurs 12',       'Charleroi',   '6000', 'Hainaut',        TRUE,  TRUE),
('CUST-010', 'Loodgieter Wouters',              'plumber',      'Diestsestraat 88',         'Leuven',      '3000', 'Flemish Brabant',TRUE,  TRUE),
('CUST-011', 'Hydraulique Moderne SA',          'plumber',      'Rue Haute 201',            'Mons',        '7000', 'Hainaut',        TRUE,  TRUE),
('CUST-012', 'Installatietechniek Bogaert',     'contractor',   'Gentsesteenweg 441',       'Brussel',     '1080', 'Brussels',       TRUE,  TRUE),
('CUST-013', 'Bouwbedrijf Hermans NV',          'contractor',   'Industriezone Noord 7',    'Hasselt',     '3500', 'Limburg',        TRUE,  TRUE),
('CUST-014', 'Vercammen Sanitair',              'plumber',      'Mechelsesteenweg 302',     'Antwerpen',   '2018', 'Antwerp',        TRUE,  TRUE),
('CUST-015', 'Bureau dArchitecture Fonteneau', 'architect',    'Avenue Louise 149',        'Brussel',     '1050', 'Brussels',       TRUE,  TRUE),
('CUST-016', 'Thermotech BVBA',                'contractor',   'Ambachtsweg 18',           'Roeselare',   '8800', 'West Flanders',  TRUE,  TRUE),
('CUST-017', 'Installaties Nijs & Partners',   'contractor',   'Rijksweg 554',             'Tongeren',    '3700', 'Limburg',        TRUE,  TRUE),
('CUST-018', 'Maison Lecomte SA',              'individual',   'Rue Saint-Lambert 33',     'Liège',       '4000', 'Liège',          FALSE, TRUE),
('CUST-019', 'RenovCo Brussel BVBA',           'contractor',   'Ninoofsesteenweg 189',     'Brussel',     '1070', 'Brussels',       TRUE,  TRUE),
('CUST-020', 'Plomberie Dumont',               'plumber',      'Chaussée de Bruxelles 77', 'Charleroi',   '6000', 'Hainaut',        TRUE,  TRUE),
('CUST-021', 'Sanitech Gent',                  'plumber',      'Wondelgemstraat 145',      'Gent',        '9000', 'East Flanders',  TRUE,  TRUE),
('CUST-022', 'Bouwaannemers Smeets BVBA',      'contractor',   'Hasseltse Dijk 88',        'Hasselt',     '3500', 'Limburg',        TRUE,  TRUE),
('CUST-023', 'Installateurs Goossens NV',      'contractor',   'Aarschotsesteenweg 23',    'Leuven',      '3000', 'Flemish Brabant',TRUE,  TRUE),
('CUST-024', 'Aqua Service Bruxelles',         'plumber',      'Rue de la Loi 88',         'Brussel',     '1040', 'Brussels',       TRUE,  TRUE),
('CUST-025', 'Klimaatcomfort BVBA',            'contractor',   'Antwerpsestraat 213',      'Mechelen',    '2800', 'Antwerp',        TRUE,  TRUE),
('CUST-026', 'Loodgieterij Baert',             'plumber',      'Bruggestraat 59',          'Kortrijk',    '8500', 'West Flanders',  TRUE,  TRUE),
('CUST-027', 'Technisat Wallonie SPRL',        'contractor',   'Rue de Liège 340',         'Namur',       '5000', 'Namur',          TRUE,  TRUE),
('CUST-028', 'Installatiebedrijf Lemmens',     'contractor',   'Tongerseweg 178',          'Hasselt',     '3500', 'Limburg',        TRUE,  TRUE),
('CUST-029', 'Energieservice Dejonghe',        'contractor',   'Vlamingenstraat 67',       'Leuven',      '3000', 'Flemish Brabant',TRUE,  TRUE),
('CUST-030', 'Dupont Plomberie',               'plumber',      'Rue Bertrand 12',          'Mons',        '7000', 'Hainaut',        TRUE,  TRUE),
('CUST-031', 'De Mol Sanitair BVBA',           'retailer',     'Steenweg op Brussel 401',  'Aalst',       '9300', 'East Flanders',  TRUE,  TRUE),
('CUST-032', 'Bouwunie Brussel',               'contractor',   'Heizel Esplanade 1',       'Brussel',     '1020', 'Brussels',       TRUE,  TRUE),
('CUST-033', 'Saniservice Vandenberghe',       'plumber',      'Stationsstraat 112',       'Roeselare',   '8800', 'West Flanders',  TRUE,  TRUE),
('CUST-034', 'Chauffage Pirard SA',            'contractor',   'Rue Ernest Solvay 55',     'Liège',       '4000', 'Liège',          TRUE,  TRUE),
('CUST-035', 'Installateur Cools',             'plumber',      'Bosstraat 29',             'Turnhout',    '2300', 'Antwerp',        TRUE,  TRUE),
('CUST-036', 'Gebr. Verbruggen NV',            'contractor',   'Industrieweg 84',          'Gent',        '9040', 'East Flanders',  TRUE,  TRUE),
('CUST-037', 'Plomberie Renard SPRL',          'plumber',      'Avenue de la Résistance 6','La Louvière', '7100', 'Hainaut',        TRUE,  TRUE),
('CUST-038', 'Wonzorg BVBA',                   'individual',   'Koning Albertlaan 22',     'Gent',        '9000', 'East Flanders',  FALSE, TRUE),
('CUST-039', 'Servicetechnici Willems',        'contractor',   'Zandvoortstraat 3',        'Antwerpen',   '2030', 'Antwerp',        TRUE,  TRUE),
('CUST-040', 'Installation Pro Mathieu',       'plumber',      'Rue de Charleroi 101',     'Namur',       '5000', 'Namur',          TRUE,  TRUE);

-- =============================================================================
-- 5. PRODUCT_SUPPLIERS — preferred supplier per product
-- =============================================================================

INSERT INTO product_suppliers (product_id, supplier_id, supplier_unit_cost_eur, supplier_lead_time_days, is_preferred) VALUES
-- Duravit products → SUP-004
(1,  4,  245.00,  8, TRUE),  (6,  4,  100.00,  8, TRUE),
-- Roca products → SUP-002
(2,  2,  138.00, 12, TRUE),  (7,  2,   82.00, 12, TRUE),  (11, 2,  120.00, 12, TRUE),
-- Geberit products → SUP-003
(3,  3,   92.00,  7, TRUE),  (4,  3,   56.00,  7, TRUE),
-- Grohe products → SUP-001
(5,  1,   28.00,  5, TRUE),  (9,  1,   62.00,  5, TRUE),  (10, 1,  41.00,  5, TRUE),
(12, 1,  348.00,  5, TRUE),  (13, 1,  155.00,  5, TRUE),  (14, 1,  56.00,  5, TRUE),
(15, 1,   12.00,  5, TRUE),  (43, 1,   82.00,  5, TRUE),  (44, 1,  183.00,  5, TRUE),
(45, 1,   50.00,  5, TRUE),
-- Watts products → SUP-005
(18, 5,   11.70,  6, TRUE),  (19, 5,    7.60,  6, TRUE),  (33, 5,   2.65,  6, TRUE),
(34, 5,    3.05,  6, TRUE),  (35, 5,    6.20,  6, TRUE),
-- Viega products → SUP-006
(21, 6,    3.68,  5, TRUE),  (22, 6,    5.84,  5, TRUE),  (23, 6,   8.57,  5, TRUE),
(24, 6,    2.03,  5, TRUE),  (25, 6,    3.74,  5, TRUE),  (26, 6,   2.86,  5, TRUE),
(27, 6,    4.95,  5, TRUE),  (31, 6,    9.20,  5, TRUE),  (32, 6,  13.95,  5, TRUE),
-- Hep2O/Wavin products → SUP-007
(28, 7,    0.76,  4, TRUE),  (29, 7,    1.11,  4, TRUE),  (30, 7,   1.33,  4, TRUE),
(46, 7,    2.47,  4, TRUE),  (47, 7,    3.30,  4, TRUE),  (48, 7,   1.33,  4, TRUE),
(49, 7,    2.41,  4, TRUE),
-- Caleffi products → SUP-008
(8,  9,   50.00,  3, TRUE),  (20, 8,   31.00, 10, TRUE),
-- Vaillant products → SUP-010
(16, 10, 120.00, 14, TRUE),  (17, 10,  63.00, 14, TRUE),  (40, 10, 247.00, 14, TRUE),
(41, 10, 348.00, 14, TRUE),  (42, 10, 443.00, 14, TRUE),
-- Sanitec products → SUP-009
(36, 9,   1.52,  3, TRUE),  (37, 9,   1.97,  3, TRUE),   (38, 9,   0.76,  3, TRUE),
(39, 9,   2.41,  3, TRUE),  (50, 9,   4.12,  3, TRUE);

-- secondary suppliers for key products (resilience)
INSERT INTO product_suppliers (product_id, supplier_id, supplier_unit_cost_eur, supplier_lead_time_days, is_preferred) VALUES
(1,  2,  255.00, 14, FALSE),
(9,  5,   68.00,  6, FALSE),
(21, 7,    4.10,  4, FALSE),
(22, 7,    6.50,  4, FALSE),
(16, 8,  128.00, 12, FALSE),
(3,  1,   98.00,  6, FALSE);

-- =============================================================================
-- 6. INVENTORY — initial stock levels (as of 2024-10-01)
-- =============================================================================
-- Strategy:
--   Warehouses (WH-LGE id=4, WH-CHA id=5): high stock, all 50 products
--   Sanicenters (BRU id=1, ANT id=2, GNT id=3): moderate stock, all 50 products
--
-- reorder_point = avg_daily_demand * lead_time_days + safety_stock
-- We encode sensible values per product category
-- =============================================================================

INSERT INTO inventory (product_id, location_id, qty_on_hand, qty_reserved, min_stock_level, reorder_point, max_stock_level) VALUES
-- LOC-BRU (location_id = 1) — Sanicenter Brussel
(1,  1,  8.00, 1.00,  2.00,  4.00,  20.00),
(2,  1, 12.00, 2.00,  3.00,  6.00,  25.00),
(3,  1, 15.00, 2.00,  4.00,  8.00,  40.00),
(4,  1, 18.00, 1.00,  4.00,  8.00,  40.00),
(5,  1, 30.00, 3.00,  8.00, 15.00,  80.00),
(6,  1,  6.00, 1.00,  2.00,  4.00,  15.00),
(7,  1, 10.00, 1.00,  2.00,  5.00,  20.00),
(8,  1, 14.00, 0.00,  3.00,  5.00,  30.00),
(9,  1, 22.00, 2.00,  5.00, 10.00,  50.00),
(10, 1, 25.00, 2.00,  5.00, 10.00,  50.00),
(11, 1,  7.00, 1.00,  2.00,  5.00,  18.00),
(12, 1,  5.00, 1.00,  1.00,  3.00,  12.00),
(13, 1,  8.00, 1.00,  2.00,  4.00,  18.00),
(14, 1, 16.00, 2.00,  4.00,  8.00,  35.00),
(15, 1, 45.00, 5.00, 10.00, 20.00, 100.00),
(16, 1,  6.00, 1.00,  1.00,  3.00,  15.00),
(17, 1,  8.00, 1.00,  2.00,  4.00,  20.00),
(18, 1, 55.00, 5.00, 15.00, 30.00, 150.00),
(19, 1, 80.00, 8.00, 20.00, 40.00, 200.00),
(20, 1, 10.00, 1.00,  2.00,  5.00,  25.00),
(21, 1,120.00,10.00, 30.00, 60.00, 300.00),
(22, 1, 80.00, 8.00, 20.00, 40.00, 200.00),
(23, 1, 60.00, 5.00, 15.00, 30.00, 150.00),
(24, 1,200.00,20.00, 50.00,100.00, 500.00),
(25, 1,150.00,15.00, 40.00, 80.00, 400.00),
(26, 1,180.00,15.00, 45.00, 90.00, 450.00),
(27, 1,130.00,12.00, 35.00, 70.00, 350.00),
(28, 1,500.00,40.00,100.00,200.00,1200.00),
(29, 1,400.00,35.00, 80.00,160.00,1000.00),
(30, 1,350.00,30.00, 70.00,140.00, 900.00),
(31, 1, 40.00, 4.00, 10.00, 20.00, 100.00),
(32, 1, 35.00, 3.00,  8.00, 16.00,  80.00),
(33, 1,100.00, 8.00, 25.00, 50.00, 250.00),
(34, 1, 90.00, 8.00, 20.00, 40.00, 200.00),
(35, 1, 70.00, 6.00, 18.00, 35.00, 180.00),
(36, 1, 80.00, 6.00, 20.00, 35.00, 200.00),
(37, 1, 70.00, 5.00, 18.00, 30.00, 180.00),
(38, 1,200.00,15.00, 50.00,100.00, 500.00),
(39, 1,120.00,10.00, 30.00, 60.00, 300.00),
(40, 1,  4.00, 1.00,  1.00,  3.00,  10.00),
(41, 1,  3.00, 0.00,  1.00,  2.00,   8.00),
(42, 1,  2.00, 0.00,  1.00,  2.00,   6.00),
(43, 1, 12.00, 1.00,  3.00,  6.00,  28.00),
(44, 1,  6.00, 1.00,  2.00,  3.00,  14.00),
(45, 1, 20.00, 2.00,  5.00, 10.00,  50.00),
(46, 1, 90.00, 8.00, 20.00, 40.00, 220.00),
(47, 1, 70.00, 6.00, 15.00, 30.00, 180.00),
(48, 1,150.00,12.00, 35.00, 70.00, 380.00),
(49, 1,120.00,10.00, 28.00, 56.00, 300.00),
(50, 1, 60.00, 5.00, 15.00, 30.00, 150.00),
-- LOC-ANT (location_id = 2) — Sanicenter Antwerpen
(1,  2, 10.00, 1.00,  2.00,  4.00,  20.00),
(2,  2, 14.00, 2.00,  3.00,  6.00,  25.00),
(3,  2, 18.00, 2.00,  4.00,  8.00,  40.00),
(4,  2, 20.00, 2.00,  4.00,  8.00,  40.00),
(5,  2, 35.00, 4.00,  8.00, 15.00,  80.00),
(6,  2,  8.00, 1.00,  2.00,  4.00,  15.00),
(7,  2, 12.00, 1.00,  2.00,  5.00,  20.00),
(8,  2, 16.00, 1.00,  3.00,  5.00,  30.00),
(9,  2, 28.00, 3.00,  5.00, 10.00,  50.00),
(10, 2, 28.00, 2.00,  5.00, 10.00,  50.00),
(11, 2,  8.00, 1.00,  2.00,  5.00,  18.00),
(12, 2,  6.00, 1.00,  1.00,  3.00,  12.00),
(13, 2,  9.00, 1.00,  2.00,  4.00,  18.00),
(14, 2, 18.00, 2.00,  4.00,  8.00,  35.00),
(15, 2, 50.00, 5.00, 10.00, 20.00, 100.00),
(16, 2,  7.00, 1.00,  1.00,  3.00,  15.00),
(17, 2,  9.00, 1.00,  2.00,  4.00,  20.00),
(18, 2, 65.00, 6.00, 15.00, 30.00, 150.00),
(19, 2, 90.00, 9.00, 20.00, 40.00, 200.00),
(20, 2, 12.00, 1.00,  2.00,  5.00,  25.00),
(21, 2,140.00,12.00, 30.00, 60.00, 300.00),
(22, 2, 95.00, 9.00, 20.00, 40.00, 200.00),
(23, 2, 70.00, 6.00, 15.00, 30.00, 150.00),
(24, 2,220.00,22.00, 50.00,100.00, 500.00),
(25, 2,170.00,17.00, 40.00, 80.00, 400.00),
(26, 2,200.00,18.00, 45.00, 90.00, 450.00),
(27, 2,145.00,14.00, 35.00, 70.00, 350.00),
(28, 2,560.00,45.00,100.00,200.00,1200.00),
(29, 2,450.00,40.00, 80.00,160.00,1000.00),
(30, 2,390.00,35.00, 70.00,140.00, 900.00),
(31, 2, 45.00, 5.00, 10.00, 20.00, 100.00),
(32, 2, 38.00, 4.00,  8.00, 16.00,  80.00),
(33, 2,110.00, 9.00, 25.00, 50.00, 250.00),
(34, 2,100.00, 9.00, 20.00, 40.00, 200.00),
(35, 2, 80.00, 7.00, 18.00, 35.00, 180.00),
(36, 2, 90.00, 7.00, 20.00, 35.00, 200.00),
(37, 2, 80.00, 6.00, 18.00, 30.00, 180.00),
(38, 2,220.00,18.00, 50.00,100.00, 500.00),
(39, 2,135.00,11.00, 30.00, 60.00, 300.00),
(40, 2,  5.00, 1.00,  1.00,  3.00,  10.00),
(41, 2,  4.00, 0.00,  1.00,  2.00,   8.00),
(42, 2,  3.00, 0.00,  1.00,  2.00,   6.00),
(43, 2, 14.00, 1.00,  3.00,  6.00,  28.00),
(44, 2,  7.00, 1.00,  2.00,  3.00,  14.00),
(45, 2, 22.00, 2.00,  5.00, 10.00,  50.00),
(46, 2,100.00, 9.00, 20.00, 40.00, 220.00),
(47, 2, 80.00, 7.00, 15.00, 30.00, 180.00),
(48, 2,165.00,14.00, 35.00, 70.00, 380.00),
(49, 2,135.00,12.00, 28.00, 56.00, 300.00),
(50, 2, 65.00, 6.00, 15.00, 30.00, 150.00),
-- LOC-GNT (location_id = 3) — Sanicenter Gent
(1,  3,  7.00, 1.00,  2.00,  4.00,  20.00),
(2,  3, 10.00, 1.00,  3.00,  6.00,  25.00),
(3,  3, 13.00, 1.00,  4.00,  8.00,  40.00),
(4,  3, 16.00, 1.00,  4.00,  8.00,  40.00),
(5,  3, 28.00, 3.00,  8.00, 15.00,  80.00),
(6,  3,  5.00, 0.00,  2.00,  4.00,  15.00),
(7,  3,  8.00, 1.00,  2.00,  5.00,  20.00),
(8,  3, 12.00, 0.00,  3.00,  5.00,  30.00),
(9,  3, 20.00, 2.00,  5.00, 10.00,  50.00),
(10, 3, 22.00, 2.00,  5.00, 10.00,  50.00),
(11, 3,  6.00, 1.00,  2.00,  5.00,  18.00),
(12, 3,  4.00, 0.00,  1.00,  3.00,  12.00),
(13, 3,  7.00, 1.00,  2.00,  4.00,  18.00),
(14, 3, 14.00, 1.00,  4.00,  8.00,  35.00),
(15, 3, 40.00, 4.00, 10.00, 20.00, 100.00),
(16, 3,  5.00, 0.00,  1.00,  3.00,  15.00),
(17, 3,  7.00, 1.00,  2.00,  4.00,  20.00),
(18, 3, 50.00, 5.00, 15.00, 30.00, 150.00),
(19, 3, 75.00, 7.00, 20.00, 40.00, 200.00),
(20, 3,  9.00, 1.00,  2.00,  5.00,  25.00),
(21, 3,110.00, 9.00, 30.00, 60.00, 300.00),
(22, 3, 75.00, 7.00, 20.00, 40.00, 200.00),
(23, 3, 55.00, 5.00, 15.00, 30.00, 150.00),
(24, 3,180.00,18.00, 50.00,100.00, 500.00),
(25, 3,140.00,14.00, 40.00, 80.00, 400.00),
(26, 3,160.00,14.00, 45.00, 90.00, 450.00),
(27, 3,120.00,11.00, 35.00, 70.00, 350.00),
(28, 3,450.00,38.00,100.00,200.00,1200.00),
(29, 3,360.00,32.00, 80.00,160.00,1000.00),
(30, 3,310.00,28.00, 70.00,140.00, 900.00),
(31, 3, 35.00, 3.00, 10.00, 20.00, 100.00),
(32, 3, 30.00, 3.00,  8.00, 16.00,  80.00),
(33, 3, 88.00, 7.00, 25.00, 50.00, 250.00),
(34, 3, 80.00, 7.00, 20.00, 40.00, 200.00),
(35, 3, 65.00, 5.00, 18.00, 35.00, 180.00),
(36, 3, 72.00, 5.00, 20.00, 35.00, 200.00),
(37, 3, 62.00, 4.00, 18.00, 30.00, 180.00),
(38, 3,180.00,14.00, 50.00,100.00, 500.00),
(39, 3,110.00, 9.00, 30.00, 60.00, 300.00),
(40, 3,  3.00, 0.00,  1.00,  3.00,  10.00),
(41, 3,  2.00, 0.00,  1.00,  2.00,   8.00),
(42, 3,  2.00, 0.00,  1.00,  2.00,   6.00),
(43, 3, 10.00, 1.00,  3.00,  6.00,  28.00),
(44, 3,  5.00, 0.00,  2.00,  3.00,  14.00),
(45, 3, 18.00, 2.00,  5.00, 10.00,  50.00),
(46, 3, 80.00, 7.00, 20.00, 40.00, 220.00),
(47, 3, 62.00, 5.00, 15.00, 30.00, 180.00),
(48, 3,138.00,11.00, 35.00, 70.00, 380.00),
(49, 3,110.00, 9.00, 28.00, 56.00, 300.00),
(50, 3, 55.00, 4.00, 15.00, 30.00, 150.00),
-- WH-LGE (location_id = 4) — Regionaal Warehouse Liège  (high stock)
(1,  4, 45.00, 5.00, 10.00, 20.00,  80.00),
(2,  4, 60.00, 6.00, 15.00, 30.00, 120.00),
(3,  4, 80.00, 8.00, 20.00, 40.00, 200.00),
(4,  4, 90.00, 9.00, 20.00, 40.00, 200.00),
(5,  4,150.00,15.00, 40.00, 80.00, 400.00),
(6,  4, 40.00, 4.00, 10.00, 20.00,  80.00),
(7,  4, 55.00, 5.00, 12.00, 25.00, 100.00),
(8,  4, 70.00, 6.00, 15.00, 30.00, 150.00),
(9,  4,100.00,10.00, 25.00, 50.00, 250.00),
(10, 4,110.00,10.00, 25.00, 50.00, 250.00),
(11, 4, 38.00, 4.00, 10.00, 20.00,  80.00),
(12, 4, 28.00, 3.00,  6.00, 12.00,  60.00),
(13, 4, 42.00, 4.00, 10.00, 20.00,  80.00),
(14, 4, 75.00, 7.00, 18.00, 36.00, 160.00),
(15, 4,220.00,20.00, 50.00,100.00, 500.00),
(16, 4, 35.00, 3.00,  8.00, 16.00,  70.00),
(17, 4, 45.00, 4.00, 10.00, 20.00,  90.00),
(18, 4,280.00,25.00, 70.00,140.00, 700.00),
(19, 4,400.00,35.00,100.00,200.00,1000.00),
(20, 4, 50.00, 5.00, 12.00, 24.00, 120.00),
(21, 4,600.00,50.00,150.00,300.00,1500.00),
(22, 4,450.00,40.00,100.00,200.00,1000.00),
(23, 4,350.00,30.00, 80.00,160.00, 800.00),
(24, 4,1000.00,80.00,250.00,500.00,2500.00),
(25, 4,800.00,70.00,200.00,400.00,2000.00),
(26, 4,900.00,75.00,220.00,440.00,2200.00),
(27, 4,700.00,60.00,180.00,360.00,1800.00),
(28, 4,2500.00,200.00,600.00,1200.00,6000.00),
(29, 4,2000.00,170.00,500.00,1000.00,5000.00),
(30, 4,1800.00,150.00,450.00,900.00,4500.00),
(31, 4,200.00,18.00, 50.00,100.00, 500.00),
(32, 4,170.00,15.00, 40.00, 80.00, 400.00),
(33, 4,500.00,40.00,120.00,240.00,1200.00),
(34, 4,450.00,38.00,110.00,220.00,1100.00),
(35, 4,360.00,30.00, 90.00,180.00, 900.00),
(36, 4,400.00,32.00,100.00,180.00,1000.00),
(37, 4,350.00,28.00, 88.00,160.00, 880.00),
(38, 4,1000.00,80.00,250.00,500.00,2500.00),
(39, 4,600.00,50.00,150.00,300.00,1500.00),
(40, 4, 20.00, 2.00,  5.00, 10.00,  50.00),
(41, 4, 15.00, 1.00,  4.00,  8.00,  38.00),
(42, 4, 10.00, 1.00,  3.00,  6.00,  28.00),
(43, 4, 60.00, 5.00, 15.00, 30.00, 140.00),
(44, 4, 30.00, 3.00,  8.00, 15.00,  70.00),
(45, 4,100.00, 9.00, 25.00, 50.00, 250.00),
(46, 4,450.00,38.00,100.00,200.00,1100.00),
(47, 4,380.00,32.00, 80.00,160.00, 900.00),
(48, 4,750.00,60.00,180.00,360.00,1800.00),
(49, 4,600.00,50.00,140.00,280.00,1500.00),
(50, 4,300.00,25.00, 75.00,150.00, 750.00),
-- WH-CHA (location_id = 5) — Regionaal Warehouse Charleroi
(1,  5, 40.00, 4.00, 10.00, 20.00,  80.00),
(2,  5, 55.00, 5.00, 15.00, 30.00, 120.00),
(3,  5, 75.00, 7.00, 20.00, 40.00, 200.00),
(4,  5, 85.00, 8.00, 20.00, 40.00, 200.00),
(5,  5,140.00,14.00, 40.00, 80.00, 400.00),
(6,  5, 36.00, 3.00, 10.00, 20.00,  80.00),
(7,  5, 50.00, 5.00, 12.00, 25.00, 100.00),
(8,  5, 65.00, 5.00, 15.00, 30.00, 150.00),
(9,  5, 92.00, 9.00, 25.00, 50.00, 250.00),
(10, 5,100.00, 9.00, 25.00, 50.00, 250.00),
(11, 5, 35.00, 3.00, 10.00, 20.00,  80.00),
(12, 5, 25.00, 2.00,  6.00, 12.00,  60.00),
(13, 5, 38.00, 3.00, 10.00, 20.00,  80.00),
(14, 5, 70.00, 6.00, 18.00, 36.00, 160.00),
(15, 5,200.00,18.00, 50.00,100.00, 500.00),
(16, 5, 32.00, 3.00,  8.00, 16.00,  70.00),
(17, 5, 42.00, 4.00, 10.00, 20.00,  90.00),
(18, 5,260.00,22.00, 70.00,140.00, 700.00),
(19, 5,370.00,32.00,100.00,200.00,1000.00),
(20, 5, 46.00, 4.00, 12.00, 24.00, 120.00),
(21, 5,560.00,46.00,150.00,300.00,1500.00),
(22, 5,420.00,37.00,100.00,200.00,1000.00),
(23, 5,325.00,28.00, 80.00,160.00, 800.00),
(24, 5,920.00,74.00,250.00,500.00,2500.00),
(25, 5,750.00,65.00,200.00,400.00,2000.00),
(26, 5,840.00,70.00,220.00,440.00,2200.00),
(27, 5,650.00,55.00,180.00,360.00,1800.00),
(28, 5,2300.00,185.00,600.00,1200.00,6000.00),
(29, 5,1850.00,158.00,500.00,1000.00,5000.00),
(30, 5,1680.00,140.00,450.00,900.00,4500.00),
(31, 5,185.00,16.00, 50.00,100.00, 500.00),
(32, 5,158.00,14.00, 40.00, 80.00, 400.00),
(33, 5,465.00,37.00,120.00,240.00,1200.00),
(34, 5,418.00,35.00,110.00,220.00,1100.00),
(35, 5,335.00,28.00, 90.00,180.00, 900.00),
(36, 5,372.00,30.00,100.00,180.00,1000.00),
(37, 5,326.00,26.00, 88.00,160.00, 880.00),
(38, 5,930.00,74.00,250.00,500.00,2500.00),
(39, 5,558.00,46.00,150.00,300.00,1500.00),
(40, 5, 18.00, 1.00,  5.00, 10.00,  50.00),
(41, 5, 14.00, 1.00,  4.00,  8.00,  38.00),
(42, 5,  9.00, 1.00,  3.00,  6.00,  28.00),
(43, 5, 56.00, 4.00, 15.00, 30.00, 140.00),
(44, 5, 28.00, 2.00,  8.00, 15.00,  70.00),
(45, 5, 93.00, 8.00, 25.00, 50.00, 250.00),
(46, 5,418.00,35.00,100.00,200.00,1100.00),
(47, 5,354.00,30.00, 80.00,160.00, 900.00),
(48, 5,698.00,56.00,180.00,360.00,1800.00),
(49, 5,558.00,46.00,140.00,280.00,1500.00),
(50, 5,279.00,23.00, 75.00,150.00, 750.00);

-- =============================================================================
-- 7. INVENTORY_MOVEMENTS — initial stock entries (2024-10-01)
-- =============================================================================

INSERT INTO inventory_movements (product_id, location_id, movement_type, qty_delta, ref_order_id, ref_order_type, movement_ts, notes)
SELECT
    i.product_id,
    i.location_id,
    'initial_stock',
    i.qty_on_hand,
    NULL,
    NULL,
    '2024-10-01 07:00:00'::TIMESTAMP,
    'Opening stock balance 2024-10-01'
FROM inventory i;

-- =============================================================================
-- 8. PURCHASE ORDERS — replenishment history Oct–Dec 2024
-- =============================================================================

INSERT INTO purchase_orders (supplier_id, location_id, created_at, expected_delivery, actual_delivery, status, total_cost_eur) VALUES
-- October replenishments
(1,  4, '2024-10-03 08:30:00', '2024-10-08',  '2024-10-08',  'received',  4820.00),
(3,  4, '2024-10-05 09:00:00', '2024-10-12',  '2024-10-13',  'received',  3240.00),
(6,  5, '2024-10-07 10:15:00', '2024-10-12',  '2024-10-11',  'received',  8750.00),
(2,  4, '2024-10-10 08:00:00', '2024-10-22',  '2024-10-23',  'received',  5600.00),
(7,  5, '2024-10-14 11:00:00', '2024-10-18',  '2024-10-18',  'received', 12300.00),
(9,  4, '2024-10-15 09:30:00', '2024-10-18',  '2024-10-17',  'received',  2180.00),
(10, 5, '2024-10-17 08:45:00', '2024-10-31',  '2024-11-02',  'received',  9850.00),
(5,  4, '2024-10-20 10:00:00', '2024-10-26',  '2024-10-26',  'received',  3420.00),
(1,  5, '2024-10-22 09:15:00', '2024-10-27',  '2024-10-27',  'received',  5240.00),
(6,  4, '2024-10-25 08:30:00', '2024-10-30',  '2024-10-31',  'received',  7680.00),
-- November replenishments
(3,  5, '2024-11-04 09:00:00', '2024-11-11',  '2024-11-11',  'received',  4120.00),
(7,  4, '2024-11-06 10:30:00', '2024-11-10',  '2024-11-09',  'received', 15400.00),
(2,  5, '2024-11-08 08:00:00', '2024-11-20',  '2024-11-21',  'received',  6300.00),
(9,  4, '2024-11-12 09:30:00', '2024-11-15',  '2024-11-14',  'received',  1980.00),
(1,  5, '2024-11-14 08:45:00', '2024-11-19',  '2024-11-19',  'received',  6180.00),
(10, 4, '2024-11-18 09:00:00', '2024-12-02',  '2024-12-04',  'received', 11200.00),
(6,  5, '2024-11-20 10:15:00', '2024-11-25',  '2024-11-25',  'received',  9340.00),
(5,  4, '2024-11-25 08:30:00', '2024-12-01',  '2024-12-01',  'received',  4100.00),
-- December replenishments
(7,  5, '2024-12-02 10:00:00', '2024-12-06',  '2024-12-06',  'received', 18600.00),
(3,  4, '2024-12-04 09:15:00', '2024-12-11',  '2024-12-12',  'received',  5280.00),
(1,  4, '2024-12-06 08:30:00', '2024-12-11',  '2024-12-11',  'received',  7340.00),
(9,  5, '2024-12-09 09:00:00', '2024-12-12',  '2024-12-11',  'received',  3150.00),
(2,  4, '2024-12-10 08:00:00', '2024-12-22',  '2024-12-23',  'received',  7200.00),
(6,  4, '2024-12-13 10:30:00', '2024-12-18',  '2024-12-18',  'received', 10240.00),
(10, 5, '2024-12-16 09:00:00', '2024-12-30',  NULL,           'confirmed', 13500.00),
(5,  5, '2024-12-18 08:45:00', '2024-12-24',  '2024-12-24',  'received',  5620.00),
(7,  4, '2024-12-20 10:00:00', '2024-12-24',  '2024-12-24',  'received', 16800.00);

-- =============================================================================
-- 9. PURCHASE ORDER LINES
-- =============================================================================

INSERT INTO purchase_order_lines (po_id, product_id, qty_ordered, qty_received, unit_cost_eur, line_total_eur) VALUES
-- PO 1 (Grohe → WH-LGE)
(1, 5,  100.00, 100.00,  28.00,  2800.00),
(1, 9,   22.00,  22.00,  62.00,  1364.00),
(1, 14,  50.00,  50.00,  56.00,  2800.00),
-- PO 2 (Geberit → WH-LGE)
(2, 3,  100.00, 100.00,  92.00,  9200.00),
(2, 4,   80.00,  80.00,  56.00,  4480.00),
-- PO 3 (Viega → WH-CHA)
(3, 21, 500.00, 500.00,   3.68,  1840.00),
(3, 22, 300.00, 300.00,   5.84,  1752.00),
(3, 24, 800.00, 800.00,   2.03,  1624.00),
(3, 25, 600.00, 600.00,   3.74,  2244.00),
(3, 31, 100.00, 100.00,   9.20,   920.00),
-- PO 4 (Roca → WH-LGE)
(4, 2,   40.00,  40.00, 138.00,  5520.00),
(4, 11,  25.00,  25.00, 120.00,  3000.00),
-- PO 5 (Wavin → WH-CHA)
(5, 28, 2000.00,2000.00,  0.76,  1520.00),
(5, 29, 1500.00,1500.00,  1.11,  1665.00),
(5, 30, 1500.00,1500.00,  1.33,  1995.00),
(5, 46,  800.00, 800.00,  2.47,  1976.00),
(5, 47,  600.00, 600.00,  3.30,  1980.00),
(5, 48, 1000.00,1000.00,  1.33,  1330.00),
(5, 49,  800.00, 800.00,  2.41,  1928.00),
-- PO 6 (Sanitec → WH-LGE)
(6, 38, 500.00, 500.00,   0.76,   380.00),
(6, 39, 200.00, 200.00,   2.41,   482.00),
(6, 50, 200.00, 200.00,   4.12,   824.00),
-- PO 7 (Vaillant → WH-CHA)
(7, 16,  20.00,  20.00, 120.00,  2400.00),
(7, 17,  30.00,  30.00,  63.00,  1890.00),
(7, 40,  15.00,  15.00, 247.00,  3705.00),
(7, 41,  10.00,  10.00, 348.00,  3480.00),
-- PO 8 (Watts → WH-LGE)
(8, 18, 200.00, 200.00,  11.70,  2340.00),
(8, 19, 100.00, 100.00,   7.60,   760.00),
(8, 33, 120.00, 120.00,   2.65,   318.00),
-- PO 9 (Grohe → WH-CHA)
(9, 5,  100.00, 100.00,  28.00,  2800.00),
(9, 13,  20.00,  20.00, 155.00,  3100.00),
(9, 15,  100.00,100.00,  12.00,  1200.00),
-- PO 10 (Viega → WH-LGE)
(10, 26, 600.00,600.00,   2.86,  1716.00),
(10, 27, 400.00,400.00,   4.95,  1980.00),
(10, 32,  80.00, 80.00,  13.95,  1116.00),
-- PO 11-28: abbreviated lines for brevity
(11, 3,   80.00,  80.00,  92.00,  7360.00),
(11, 4,   60.00,  60.00,  56.00,  3360.00),
(12, 28, 2500.00,2500.00, 0.76,  1900.00),
(12, 29, 2000.00,2000.00, 1.11,  2220.00),
(12, 46, 1000.00,1000.00, 2.47,  2470.00),
(12, 47,  800.00, 800.00, 3.30,  2640.00),
(13, 2,   45.00,  45.00,138.00,  6210.00),
(13, 7,   30.00,  30.00, 82.00,  2460.00),
(14, 38, 400.00, 400.00,  0.76,   304.00),
(14, 50, 180.00, 180.00,  4.12,   741.00),
(15, 5,  120.00, 120.00, 28.00,  3360.00),
(15, 9,   30.00,  30.00, 62.00,  1860.00),
(15, 14,  60.00,  60.00, 56.00,  3360.00),
(16, 16,  25.00,  25.00,120.00,  3000.00),
(16, 17,  35.00,  35.00, 63.00,  2205.00),
(16, 40,  18.00,  18.00,247.00,  4446.00),
(17, 21, 600.00, 600.00,  3.68,  2208.00),
(17, 22, 400.00, 400.00,  5.84,  2336.00),
(17, 24, 900.00, 900.00,  2.03,  1827.00),
(18, 18, 180.00, 180.00, 11.70,  2106.00),
(18, 33, 100.00, 100.00,  2.65,   265.00),
(19, 28,3000.00,3000.00,  0.76,  2280.00),
(19, 30,2000.00,2000.00,  1.33,  2660.00),
(19, 46,1200.00,1200.00,  2.47,  2964.00),
(20, 3,   90.00,  90.00, 92.00,  8280.00),
(20, 4,   70.00,  70.00, 56.00,  3920.00),
(21, 5,  130.00, 130.00, 28.00,  3640.00),
(21, 43,  50.00,  50.00, 82.00,  4100.00),
(22, 38, 500.00, 500.00,  0.76,   380.00),
(22, 50, 220.00, 220.00,  4.12,   906.00),
(23, 2,   50.00,  50.00,138.00,  6900.00),
(23, 11,  30.00,  30.00,120.00,  3600.00),
(24, 21, 700.00, 700.00,  3.68,  2576.00),
(24, 26, 700.00, 700.00,  2.86,  2002.00),
(24, 27, 500.00, 500.00,  4.95,  2475.00),
(25, 16,  30.00,   0.00,120.00,  3600.00),
(25, 17,  40.00,   0.00, 63.00,  2520.00),
(25, 42,   8.00,   0.00,443.00,  3544.00),
(26, 18, 200.00, 200.00, 11.70,  2340.00),
(26, 35, 150.00, 150.00,  6.20,   930.00),
(27, 28,2800.00,2800.00,  0.76,  2128.00),
(27, 29,2200.00,2200.00,  1.11,  2442.00),
(27, 30,2000.00,2000.00,  1.33,  2660.00),
(27, 46,1400.00,1400.00,  2.47,  3458.00),
(27, 47,1100.00,1100.00,  3.30,  3630.00);

-- =============================================================================
-- 10. SALES ORDERS — Oct–Dec 2024
-- ~120 orders across 3 sanicenters, realistic Belgian working days
-- =============================================================================

INSERT INTO sales_orders (customer_id, location_id, order_ts, status, source, total_amount_eur) VALUES
-- October 2024
(1,  1, '2024-10-01 09:15:00', 'fulfilled', 'counter',    485.00),
(3,  3, '2024-10-01 10:30:00', 'fulfilled', 'counter',    312.50),
(7,  2, '2024-10-02 08:45:00', 'fulfilled', 'phone',      896.00),
(14, 2, '2024-10-02 11:00:00', 'fulfilled', 'counter',    178.00),
(6,  1, '2024-10-03 09:30:00', 'fulfilled', 'counter',    540.00),
(21, 3, '2024-10-03 14:15:00', 'fulfilled', 'phone',      224.50),
(2,  2, '2024-10-04 10:00:00', 'fulfilled', 'counter',    652.00),
(10, 1, '2024-10-04 13:30:00', 'fulfilled', 'online',     319.00),
(4,  2, '2024-10-07 09:00:00', 'fulfilled', 'phone',     1240.00),
(16, 3, '2024-10-07 11:15:00', 'fulfilled', 'counter',    487.00),
(5,  1, '2024-10-08 08:30:00', 'fulfilled', 'counter',    198.00),
(26, 3, '2024-10-08 15:00:00', 'fulfilled', 'counter',    356.00),
(33, 3, '2024-10-09 09:45:00', 'fulfilled', 'counter',    745.00),
(12, 1, '2024-10-09 11:30:00', 'fulfilled', 'phone',      428.00),
(39, 2, '2024-10-10 10:15:00', 'fulfilled', 'counter',    894.00),
(22, 2, '2024-10-10 14:00:00', 'fulfilled', 'online',     267.00),
(19, 1, '2024-10-11 09:00:00', 'fulfilled', 'counter',    582.00),
(36, 3, '2024-10-11 13:30:00', 'fulfilled', 'phone',      1089.00),
(1,  1, '2024-10-14 08:45:00', 'fulfilled', 'counter',    234.00),
(8,  3, '2024-10-14 10:00:00', 'fulfilled', 'counter',    678.00),
(25, 2, '2024-10-15 09:30:00', 'fulfilled', 'phone',      445.00),
(34, 1, '2024-10-15 14:15:00', 'fulfilled', 'online',     892.00),
(7,  2, '2024-10-16 10:00:00', 'fulfilled', 'counter',    336.00),
(13, 2, '2024-10-16 13:00:00', 'fulfilled', 'counter',    1450.00),
(28, 2, '2024-10-17 09:15:00', 'fulfilled', 'phone',      289.00),
(3,  3, '2024-10-17 11:30:00', 'fulfilled', 'counter',    567.00),
(15, 1, '2024-10-18 08:30:00', 'fulfilled', 'counter',    1890.00),
(40, 1, '2024-10-18 14:00:00', 'fulfilled', 'phone',      234.00),
(24, 1, '2024-10-21 09:00:00', 'fulfilled', 'online',     412.00),
(6,  1, '2024-10-21 11:15:00', 'fulfilled', 'counter',    678.00),
(31, 3, '2024-10-22 10:00:00', 'fulfilled', 'counter',    345.00),
(2,  2, '2024-10-22 13:30:00', 'fulfilled', 'counter',    789.00),
(35, 2, '2024-10-23 09:45:00', 'fulfilled', 'phone',      156.00),
(10, 1, '2024-10-23 14:00:00', 'fulfilled', 'online',     523.00),
(17, 2, '2024-10-24 10:30:00', 'fulfilled', 'counter',    890.00),
(21, 3, '2024-10-24 13:00:00', 'fulfilled', 'counter',    267.00),
(4,  1, '2024-10-25 09:00:00', 'fulfilled', 'phone',     1340.00),
(30, 1, '2024-10-25 15:00:00', 'fulfilled', 'counter',    198.00),
(11, 1, '2024-10-28 09:15:00', 'fulfilled', 'counter',    445.00),
(23, 3, '2024-10-28 11:30:00', 'fulfilled', 'phone',      678.00),
(14, 2, '2024-10-29 10:00:00', 'fulfilled', 'counter',    234.00),
(37, 1, '2024-10-29 13:45:00', 'fulfilled', 'online',     567.00),
(20, 1, '2024-10-30 09:30:00', 'fulfilled', 'counter',    890.00),
(9,  1, '2024-10-30 14:00:00', 'fulfilled', 'phone',      412.00),
(29, 3, '2024-10-31 10:15:00', 'fulfilled', 'counter',    345.00),
-- November 2024
(1,  1, '2024-11-04 09:00:00', 'fulfilled', 'counter',    678.00),
(7,  2, '2024-11-04 11:15:00', 'fulfilled', 'counter',    1234.00),
(36, 3, '2024-11-05 08:45:00', 'fulfilled', 'phone',      456.00),
(3,  3, '2024-11-05 13:30:00', 'fulfilled', 'counter',    789.00),
(25, 2, '2024-11-06 10:00:00', 'fulfilled', 'online',     345.00),
(12, 1, '2024-11-06 14:15:00', 'fulfilled', 'counter',    567.00),
(39, 2, '2024-11-07 09:30:00', 'fulfilled', 'counter',    1100.00),
(22, 2, '2024-11-07 13:00:00', 'fulfilled', 'phone',      234.00),
(15, 1, '2024-11-08 08:30:00', 'fulfilled', 'counter',    2240.00),
(34, 1, '2024-11-08 15:00:00', 'fulfilled', 'online',     678.00),
(4,  2, '2024-11-11 09:00:00', 'fulfilled', 'counter',    1560.00),
(16, 3, '2024-11-11 11:30:00', 'fulfilled', 'counter',    890.00),
(26, 3, '2024-11-12 10:00:00', 'fulfilled', 'phone',      234.00),
(2,  2, '2024-11-12 13:45:00', 'fulfilled', 'counter',    567.00),
(13, 2, '2024-11-13 09:15:00', 'fulfilled', 'counter',    1890.00),
(8,  3, '2024-11-13 14:00:00', 'fulfilled', 'phone',      445.00),
(19, 1, '2024-11-14 09:30:00', 'fulfilled', 'counter',    678.00),
(5,  1, '2024-11-14 13:00:00', 'fulfilled', 'counter',    289.00),
(10, 1, '2024-11-15 10:00:00', 'fulfilled', 'online',     456.00),
(28, 2, '2024-11-15 14:30:00', 'fulfilled', 'phone',      789.00),
(6,  1, '2024-11-18 09:00:00', 'fulfilled', 'counter',    567.00),
(33, 3, '2024-11-18 11:15:00', 'fulfilled', 'counter',    1234.00),
(40, 1, '2024-11-19 08:45:00', 'fulfilled', 'phone',      345.00),
(24, 1, '2024-11-19 13:30:00', 'fulfilled', 'online',     678.00),
(17, 2, '2024-11-20 10:00:00', 'fulfilled', 'counter',    1120.00),
(31, 3, '2024-11-20 14:00:00', 'fulfilled', 'counter',    456.00),
(9,  1, '2024-11-21 09:15:00', 'fulfilled', 'phone',      234.00),
(35, 2, '2024-11-21 13:00:00', 'fulfilled', 'counter',    567.00),
(23, 3, '2024-11-22 10:30:00', 'fulfilled', 'counter',    890.00),
(1,  1, '2024-11-22 14:15:00', 'fulfilled', 'online',     345.00),
(11, 1, '2024-11-25 09:00:00', 'fulfilled', 'counter',    678.00),
(14, 2, '2024-11-25 11:30:00', 'fulfilled', 'counter',    234.00),
(29, 3, '2024-11-26 10:00:00', 'fulfilled', 'phone',      1567.00),
(37, 1, '2024-11-26 13:45:00', 'fulfilled', 'counter',    456.00),
(20, 1, '2024-11-27 09:30:00', 'fulfilled', 'online',     789.00),
(30, 1, '2024-11-27 14:00:00', 'fulfilled', 'counter',    234.00),
(7,  2, '2024-11-28 10:15:00', 'fulfilled', 'counter',    1345.00),
(21, 3, '2024-11-28 13:00:00', 'fulfilled', 'phone',      567.00),
(18, 1, '2024-11-29 09:00:00', 'fulfilled', 'counter',    159.00),
(38, 3, '2024-11-29 14:30:00', 'fulfilled', 'counter',    289.00),
-- December 2024
(4,  2, '2024-12-02 09:00:00', 'fulfilled', 'phone',     1780.00),
(3,  3, '2024-12-02 11:15:00', 'fulfilled', 'counter',    567.00),
(12, 1, '2024-12-03 08:45:00', 'fulfilled', 'counter',    890.00),
(25, 2, '2024-12-03 13:30:00', 'fulfilled', 'online',     345.00),
(39, 2, '2024-12-04 10:00:00', 'fulfilled', 'counter',    1230.00),
(16, 3, '2024-12-04 14:15:00', 'fulfilled', 'counter',    678.00),
(15, 1, '2024-12-05 09:30:00', 'fulfilled', 'phone',     2890.00),
(2,  2, '2024-12-05 13:00:00', 'fulfilled', 'counter',    456.00),
(36, 3, '2024-12-06 10:00:00', 'fulfilled', 'counter',    789.00),
(8,  3, '2024-12-06 14:30:00', 'fulfilled', 'phone',      567.00),
(7,  2, '2024-12-09 09:00:00', 'fulfilled', 'counter',    1456.00),
(1,  1, '2024-12-09 11:15:00', 'fulfilled', 'counter',    234.00),
(22, 2, '2024-12-10 08:45:00', 'fulfilled', 'online',     678.00),
(13, 2, '2024-12-10 13:30:00', 'fulfilled', 'counter',    2100.00),
(6,  1, '2024-12-11 10:00:00', 'fulfilled', 'counter',    890.00),
(33, 3, '2024-12-11 14:00:00', 'fulfilled', 'counter',    345.00),
(19, 1, '2024-12-12 09:15:00', 'fulfilled', 'phone',      567.00),
(34, 1, '2024-12-12 13:45:00', 'fulfilled', 'online',     1234.00),
(26, 3, '2024-12-13 10:30:00', 'fulfilled', 'counter',    456.00),
(10, 1, '2024-12-13 14:00:00', 'fulfilled', 'counter',    789.00),
(4,  1, '2024-12-16 09:00:00', 'fulfilled', 'phone',     1670.00),
(17, 2, '2024-12-16 11:30:00', 'fulfilled', 'counter',    890.00),
(14, 2, '2024-12-17 10:00:00', 'fulfilled', 'counter',    234.00),
(28, 2, '2024-12-17 13:15:00', 'fulfilled', 'phone',      567.00),
(40, 1, '2024-12-18 09:30:00', 'fulfilled', 'online',     345.00),
(5,  1, '2024-12-18 14:00:00', 'fulfilled', 'counter',    456.00),
(23, 3, '2024-12-19 10:15:00', 'fulfilled', 'counter',    789.00),
(29, 3, '2024-12-19 13:30:00', 'fulfilled', 'phone',     1890.00),
(9,  1, '2024-12-20 09:00:00', 'fulfilled', 'counter',    234.00),
(11, 1, '2024-12-20 14:15:00', 'partial',   'counter',    567.00),
(24, 1, '2024-12-23 10:00:00', 'fulfilled', 'online',     890.00),
(30, 1, '2024-12-23 13:00:00', 'fulfilled', 'counter',    345.00),
(21, 3, '2024-12-26 09:15:00', 'fulfilled', 'counter',    456.00),
(37, 1, '2024-12-26 11:30:00', 'fulfilled', 'phone',      678.00),
(31, 3, '2024-12-27 10:00:00', 'fulfilled', 'counter',    234.00),
(20, 1, '2024-12-27 14:00:00', 'fulfilled', 'online',     567.00);

-- =============================================================================
-- 11. SALES ORDER LINES
-- =============================================================================

INSERT INTO sales_order_lines (order_id, product_id, qty_ordered, qty_fulfilled, unit_price_eur, line_total_eur) VALUES
-- Order 1 (Oct 1, customer 1, BRU)
(1, 9,  2.00, 2.00, 98.00, 196.00),
(1, 5,  2.00, 2.00, 45.00,  90.00),
(1, 33, 5.00, 5.00,  4.20,  21.00),
(1, 38,15.00,15.00,  1.20,  18.00),
-- Order 2 (Oct 1, customer 3, GNT)
(2, 3,  1.00, 1.00,145.00, 145.00),
(2, 4,  1.00, 1.00, 89.00,  89.00),
(2, 38,10.00,10.00,  1.20,  12.00),
(2, 39, 5.00, 5.00,  3.80,  19.00),
-- Order 3 (Oct 2, customer 7, ANT)
(3, 1,  2.00, 2.00,389.00, 778.00),
(3, 5,  2.00, 2.00, 45.00,  90.00),
-- Order 4 (Oct 2, customer 14, ANT)
(4, 9,  1.00, 1.00, 98.00,  98.00),
(4, 33, 8.00, 8.00,  4.20,  33.60),
(4, 38,20.00,20.00,  1.20,  24.00),
-- Order 5 (Oct 3, customer 6, BRU)
(5, 13, 2.00, 2.00,245.00, 490.00),
(5, 15, 2.00, 2.00, 18.50,  37.00),
-- Order 6 (Oct 3, customer 21, GNT)
(6, 24,20.00,20.00,  3.20,  64.00),
(6, 25,15.00,15.00,  5.90,  88.50),
(6, 38,15.00,15.00,  1.20,  18.00),
-- Order 7 (Oct 4, customer 2, ANT)
(7, 9,  2.00, 2.00, 98.00, 196.00),
(7, 10, 3.00, 3.00, 65.00, 195.00),
(7, 33, 5.00, 5.00,  4.20,  21.00),
(7, 34, 5.00, 5.00,  4.80,  24.00),
-- Order 8 (Oct 4, customer 10, BRU)
(8, 21,30.00,30.00,  5.80, 174.00),
(8, 22,10.00,10.00,  9.20,  92.00),
(8, 38,20.00,20.00,  1.20,  24.00),
-- Order 9 (Oct 7, customer 4, ANT) — large contractor order
(9, 3,  3.00, 3.00,145.00, 435.00),
(9, 4,  3.00, 3.00, 89.00, 267.00),
(9, 9,  2.00, 2.00, 98.00, 196.00),
(9, 24,50.00,50.00,  3.20, 160.00),
(9, 28,50.00,50.00,  1.20,  60.00),
-- Order 10 (Oct 7, customer 16, GNT)
(10, 18,10.00,10.00, 18.50, 185.00),
(10, 19,20.00,20.00, 12.00, 240.00),
-- Order 11 (Oct 8, customer 5, BRU)
(11, 5,  2.00, 2.00, 45.00,  90.00),
(11, 33, 5.00, 5.00,  4.20,  21.00),
(11, 35, 5.00, 5.00,  9.80,  49.00),
-- Order 12 (Oct 8, customer 26, GNT)
(12, 9,  1.00, 1.00, 98.00,  98.00),
(12, 14, 2.00, 2.00, 89.00, 178.00),
(12, 15, 4.00, 4.00, 18.50,  74.00),
-- Order 13 (Oct 9, customer 33, GNT)
(13, 2,  2.00, 2.00,219.00, 438.00),
(13, 3,  1.00, 1.00,145.00, 145.00),
(13, 4,  1.00, 1.00, 89.00,  89.00),
-- Order 14 (Oct 9, customer 12, BRU)
(14, 21,30.00,30.00,  5.80, 174.00),
(14, 22,15.00,15.00,  9.20, 138.00),
(14, 24,20.00,20.00,  3.20,  64.00),
-- Order 15 (Oct 10, customer 39, ANT)
(15, 16, 2.00, 2.00,189.00, 378.00),
(15, 18,10.00,10.00, 18.50, 185.00),
(15, 20, 2.00, 2.00, 49.00,  98.00),
-- Orders 16-120: representative lines keeping totals consistent
(16, 24,30.00,30.00,  3.20,  96.00),
(16, 38,20.00,20.00,  1.20,  24.00),
(16, 50, 5.00, 5.00,  6.50,  32.50),
(17, 9,  2.00, 2.00, 98.00, 196.00),
(17, 14, 4.00, 4.00, 89.00, 356.00),
(18, 21,40.00,40.00,  5.80, 232.00),
(18, 46,30.00,30.00,  3.90, 117.00),
(18, 48,50.00,50.00,  2.10, 105.00),
(19, 1,  1.00, 1.00,389.00, 389.00),
(19, 5,  2.00, 2.00, 45.00,  90.00),
(19, 38,10.00,10.00,  1.20,  12.00),
(20, 12, 1.00, 1.00,549.00, 549.00),
(20, 14, 1.00, 1.00, 89.00,  89.00),
(21, 9,  2.00, 2.00, 98.00, 196.00),
(21, 10, 2.00, 2.00, 65.00, 130.00),
(21, 15, 3.00, 3.00, 18.50,  55.50),
(22, 18,10.00,10.00, 18.50, 185.00),
(22, 19,10.00,10.00, 12.00, 120.00),
(23, 13, 1.00, 1.00,245.00, 245.00),
(23, 14, 1.00, 1.00, 89.00,  89.00),
(24, 2,  1.00, 1.00,219.00, 219.00),
(24, 16, 2.00, 2.00,189.00, 378.00),
(24, 17, 2.00, 2.00, 99.00, 198.00),
(25, 24,30.00,30.00,  3.20,  96.00),
(25, 25,20.00,20.00,  5.90, 118.00),
(26, 9,  2.00, 2.00, 98.00, 196.00),
(26, 5,  4.00, 4.00, 45.00, 180.00),
(27, 44, 2.00, 2.00,289.00, 578.00),
(27, 43, 1.00, 1.00,129.00, 129.00),
(27, 5,  2.00, 2.00, 45.00,  90.00),
(28, 5,  2.00, 2.00, 45.00,  90.00),
(28, 33, 5.00, 5.00,  4.20,  21.00),
(29, 21,40.00,40.00,  5.80, 232.00),
(30, 9,  2.00, 2.00, 98.00, 196.00),
(30, 10, 2.00, 2.00, 65.00, 130.00),
(31, 28,50.00,50.00,  1.20,  60.00),
(31, 29,30.00,30.00,  1.75,  52.50),
(31, 50, 5.00, 5.00,  6.50,  32.50),
(32, 2,  2.00, 2.00,219.00, 438.00),
(32, 5,  2.00, 2.00, 45.00,  90.00),
(33, 15, 5.00, 5.00, 18.50,  92.50),
(33, 33, 5.00, 5.00,  4.20,  21.00),
(34, 9,  2.00, 2.00, 98.00, 196.00),
(34, 43, 1.00, 1.00,129.00, 129.00),
(35, 16, 2.00, 2.00,189.00, 378.00),
(35, 20, 1.00, 1.00, 49.00,  49.00),
(36, 24,20.00,20.00,  3.20,  64.00),
(36, 38,10.00,10.00,  1.20,  12.00),
(37, 1,  2.00, 2.00,389.00, 778.00),
(37, 5,  2.00, 2.00, 45.00,  90.00),
(38, 5,  2.00, 2.00, 45.00,  90.00),
(38, 33, 5.00, 5.00,  4.20,  21.00),
(39, 18,12.00,12.00, 18.50, 222.00),
(40, 9,  2.00, 2.00, 98.00, 196.00),
(40, 14, 2.00, 2.00, 89.00, 178.00),
(41, 9,  1.00, 1.00, 98.00,  98.00),
(41, 33, 5.00, 5.00,  4.20,  21.00),
(42, 21,30.00,30.00,  5.80, 174.00),
(43, 2,  2.00, 2.00,219.00, 438.00),
(43, 16, 1.00, 1.00,189.00, 189.00),
(44, 9,  2.00, 2.00, 98.00, 196.00),
(44, 10, 2.00, 2.00, 65.00, 130.00),
(45, 28,50.00,50.00,  1.20,  60.00),
(45, 46,30.00,30.00,  3.90, 117.00);

-- Nov-Dec order lines (abbreviated for remaining orders)
INSERT INTO sales_order_lines (order_id, product_id, qty_ordered, qty_fulfilled, unit_price_eur, line_total_eur)
SELECT
    o.order_id,
    p.product_id,
    qty,
    qty,
    p.unit_price_eur,
    ROUND((qty * p.unit_price_eur)::numeric, 2)
FROM (
    VALUES
    -- Nov orders (46-85)
    (46, 9,  2.00), (46, 5,  2.00), (46, 38, 20.00),
    (47, 1,  1.00), (47, 13, 1.00), (47, 15,  2.00),
    (48, 18,10.00), (48, 19, 8.00),
    (49, 3,  2.00), (49, 4,  2.00), (49, 50,  5.00),
    (50, 28,50.00), (50, 29,30.00),
    (51, 9,  2.00), (51, 43, 1.00), (51, 15,  3.00),
    (52, 2,  2.00), (52, 11, 1.00),
    (53, 21,40.00), (53, 22,20.00), (53, 24,30.00),
    (54, 9,  2.00), (54, 10, 2.00),
    (55, 44, 2.00), (55, 43, 2.00), (55, 5,   4.00),
    (56, 18,12.00), (56, 19,10.00),
    (57, 16, 4.00), (57, 17, 4.00), (57, 20,  2.00),
    (58, 9,  2.00), (58, 33, 5.00),
    (59, 1,  2.00), (59, 5,  2.00),
    (60, 21,30.00), (60, 46,20.00),
    (61, 3,  1.00), (61, 9,  2.00), (61, 38, 10.00),
    (62, 2,  2.00), (62, 16, 2.00),
    (63, 24,40.00), (63, 26,30.00),
    (64, 9,  1.00), (64, 14, 2.00),
    (65, 16, 4.00), (65, 17, 4.00),
    (66, 9,  2.00), (66, 10, 2.00), (66, 50,  5.00),
    (67, 21,40.00), (67, 22,20.00),
    (68, 9,  2.00), (68, 5,  2.00),
    (69, 2,  2.00), (69, 6,  1.00),
    (70, 28,50.00), (70, 30,40.00),
    (71, 18,10.00), (71, 35, 8.00),
    (72, 1,  2.00), (72, 44, 1.00),
    (73, 9,  2.00), (73, 33, 8.00),
    (74, 12, 1.00), (74, 14, 1.00),
    (75, 21,30.00), (75, 24,25.00),
    (76, 16, 2.00), (76, 20, 2.00),
    (77, 9,  2.00), (77, 5,  4.00),
    (78, 2,  2.00), (78, 3,  1.00),
    (79, 24,30.00), (79, 25,20.00),
    (80, 43, 2.00), (80, 10, 2.00),
    (81, 9,  2.00), (81, 14, 4.00),
    (82, 18,12.00), (82, 19, 8.00),
    (83, 28,60.00), (83, 46,40.00),
    (84, 9,  2.00), (84, 33, 5.00),
    (85, 5,  2.00), (85, 38,10.00),
    -- Dec orders (86-121)
    (86, 16, 4.00), (86, 17, 4.00), (86, 20,  2.00),
    (87, 3,  2.00), (87, 4,  2.00),
    (88, 9,  2.00), (88, 10, 2.00), (88, 50,  8.00),
    (89, 28,50.00), (89, 29,30.00),
    (90, 1,  2.00), (90, 13, 1.00),
    (91, 18,12.00), (91, 35,10.00),
    (92, 44, 2.00), (92, 43, 2.00), (92, 5,   4.00),
    (93, 2,  2.00), (93, 11, 1.00),
    (94, 21,40.00), (94, 22,20.00),
    (95, 16, 4.00), (95, 17, 4.00),
    (96, 9,  2.00), (96, 14, 2.00),
    (97, 2,  1.00), (97, 16, 2.00),
    (98, 22,20.00), (98, 24,30.00),
    (99, 1,  1.00), (99, 12, 1.00),
    (100, 3, 1.00), (100, 4, 1.00),(100,38,10.00),
    (101, 16,4.00), (101,17, 4.00),
    (102, 9, 2.00), (102,10, 2.00),
    (103, 5, 2.00), (103,33, 5.00),(103,38,10.00),
    (104,21,40.00), (104,46,30.00),
    (105,40, 2.00), (105,16, 2.00),
    (106,28,50.00), (106,29,30.00),
    (107, 9, 2.00), (107, 5, 4.00),
    (108, 2, 2.00), (108,16, 2.00),
    (109,18,10.00), (109,19, 8.00),
    (110, 1, 2.00), (110, 5, 2.00),
    (111, 9, 1.00), (111,33, 5.00),(111,38, 5.00),
    (112,21,30.00), (112,22,15.00),
    (113, 9, 2.00), (113,10, 2.00),
    (114,28,50.00), (114,30,40.00),
    (115,43, 1.00), (115,10, 2.00),
    (116,18,12.00), (116,35, 8.00),
    (117,24,30.00), (117,25,20.00),
    (118,16, 2.00), (118,17, 2.00),
    (119, 9, 2.00), (119,14, 2.00),
    (120, 9, 2.00), (120,33, 5.00),
    (121, 2, 2.00), (121, 5, 2.00)
) AS v(oid, pid, qty)
JOIN sales_orders o ON o.order_id = v.oid
JOIN products p ON p.product_id = v.pid;

-- =============================================================================
-- 12. INVENTORY MOVEMENTS — sale movements derived from fulfilled order lines
-- =============================================================================

INSERT INTO inventory_movements (product_id, location_id, movement_type, qty_delta, ref_order_id, ref_order_type, movement_ts, notes)
SELECT
    sol.product_id,
    so.location_id,
    'sale',
    -sol.qty_fulfilled,
    so.order_id,
    'sales_order',
    so.order_ts,
    'Fulfilled from sales order ' || so.order_id
FROM sales_order_lines sol
JOIN sales_orders so ON so.order_id = sol.order_id
WHERE sol.qty_fulfilled > 0;

-- PO receipt movements
INSERT INTO inventory_movements (product_id, location_id, movement_type, qty_delta, ref_order_id, ref_order_type, movement_ts, notes)
SELECT
    pol.product_id,
    po.location_id,
    'po_receipt',
    pol.qty_received,
    po.po_id,
    'purchase_order',
    (po.actual_delivery::TIMESTAMP + INTERVAL '8 hours'),
    'Received from PO ' || po.po_id || ' — supplier ' || po.supplier_id
FROM purchase_order_lines pol
JOIN purchase_orders po ON po.po_id = pol.po_id
WHERE pol.qty_received > 0
  AND po.actual_delivery IS NOT NULL;

-- =============================================================================
-- 13. USEFUL VIEWS
-- =============================================================================

CREATE OR REPLACE VIEW v_inventory_status AS
SELECT
    l.location_name,
    l.location_type,
    l.city,
    p.sku,
    p.product_name,
    p.category,
    i.qty_on_hand,
    i.qty_reserved,
    (i.qty_on_hand - i.qty_reserved) AS qty_available,
    i.min_stock_level,
    i.reorder_point,
    i.max_stock_level,
    CASE WHEN i.qty_on_hand = 0                       THEN 'RUPTURED'
         WHEN i.qty_on_hand <= i.min_stock_level       THEN 'BELOW_MIN'
         WHEN i.qty_on_hand <= i.reorder_point         THEN 'REORDER'
         ELSE 'OK'
    END AS stock_status,
    i.last_updated
FROM inventory i
JOIN products p  ON p.product_id  = i.product_id
JOIN locations l ON l.location_id = i.location_id
ORDER BY l.location_name, p.category, p.product_name;

CREATE OR REPLACE VIEW v_daily_sales AS
SELECT
    DATE(so.order_ts)       AS sale_date,
    l.location_name,
    p.category,
    p.product_name,
    SUM(sol.qty_fulfilled)  AS total_qty_sold,
    SUM(sol.line_total_eur) AS total_revenue_eur
FROM sales_order_lines sol
JOIN sales_orders so ON so.order_id = sol.order_id
JOIN products p      ON p.product_id = sol.product_id
JOIN locations l     ON l.location_id = so.location_id
GROUP BY 1,2,3,4
ORDER BY 1 DESC, 5 DESC;

CREATE OR REPLACE VIEW v_supplier_performance AS
SELECT
    s.supplier_name,
    s.country,
    COUNT(po.po_id)                                              AS total_pos,
    AVG(po.actual_delivery - po.created_at::date)               AS avg_actual_lead_days,
    s.avg_lead_time_days                                        AS contracted_lead_days,
    ROUND(AVG(
        CASE WHEN po.actual_delivery <= po.expected_delivery
             THEN 1.0 ELSE 0.0 END
    )::numeric, 2)                                              AS on_time_rate
FROM purchase_orders po
JOIN suppliers s ON s.supplier_id = po.supplier_id
WHERE po.actual_delivery IS NOT NULL
GROUP BY s.supplier_id, s.supplier_name, s.country, s.avg_lead_time_days
ORDER BY on_time_rate DESC;
