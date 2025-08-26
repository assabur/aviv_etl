-- Supprime les tables si elles existent déjà (avec CASCADE pour forcer la suppression des dépendances)
DROP TABLE IF EXISTS public.historique_price CASCADE;
DROP TABLE IF EXISTS public.property CASCADE;
DROP TABLE IF EXISTS public.item_sub_type CASCADE;
DROP TABLE IF EXISTS public.item_type CASCADE;
DROP TABLE IF EXISTS public.transaction_type CASCADE;

-- Recréation des tables
CREATE TABLE IF NOT EXISTS public.transaction_type
(
    id_transaction_type integer PRIMARY KEY,
    libelle_transaction_type text
);

CREATE TABLE IF NOT EXISTS public.item_type
(
    id_item_type integer PRIMARY KEY,
    libelle_item_ty text
);

CREATE TABLE IF NOT EXISTS public.item_sub_type (
    id_item_sub_type integer PRIMARY KEY,
    libelle_item_sub_type text,
    id_item_type integer NOT NULL REFERENCES public.item_type(id_item_type)
        ON UPDATE CASCADE ON DELETE RESTRICT,
    CONSTRAINT uq_item_sub_type UNIQUE (id_item_type, libelle_item_sub_type)
);

CREATE TABLE IF NOT EXISTS public.property (
    id_property bigint PRIMARY KEY,
    start_date date,
    price numeric(14,2),
    area numeric(10,2),
    site_area numeric(12,2),
    floor integer,
    room_count integer,
    balcony_count integer,
    terrace_count integer,
    has_garden text,
    city text,
    zipcode text,
    has_passenger_lift text,
    is_new_construction text,
    build_year integer,
    terrace_area numeric(10,2),
    has_cellar text,

    -- Références vers les tables de type
    id_transaction_type integer NOT NULL REFERENCES public.transaction_type(id_transaction_type),
    id_item_type        integer NOT NULL REFERENCES public.item_type(id_item_type),
    id_item_sub_type    integer NOT NULL REFERENCES public.item_sub_type(id_item_sub_type)
);

-- Historique des changements de prix
CREATE TABLE IF NOT EXISTS public.historique_price (
    id_historique_price BIGINT PRIMARY KEY,
    id_property         BIGINT NOT NULL REFERENCES public.property(id_property)
                            ON UPDATE CASCADE ON DELETE CASCADE,
    change_date         date NOT NULL,
    change_price        NUMERIC(14,2) NOT NULL
);

-- Index utiles
CREATE INDEX IF NOT EXISTS idx_property_city           ON public.property (city);
CREATE INDEX IF NOT EXISTS idx_property_zipcode        ON public.property (zipcode);
CREATE INDEX IF NOT EXISTS idx_hist_price_prop_date    ON public.historique_price (id_property, change_date);

-- Index sur FKs
CREATE INDEX IF NOT EXISTS idx_property_fk_trx_type    ON public.property (id_transaction_type);
CREATE INDEX IF NOT EXISTS idx_property_fk_item_type   ON public.property (id_item_type);
CREATE INDEX IF NOT EXISTS idx_property_fk_item_sub    ON public.property (id_item_sub_type);
