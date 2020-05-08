import sqlite3
conn = sqlite3.connect('scrapped_sites.db')
c = conn.cursor()

# crea tabla sites
c.execute('create table sites (id integer PRIMARY KEY AUTOINCREMENT, name text, url text, n_visits integer)')
# crea tabla de secciones
c.execute('create table site_sections (id integer PRIMARY KEY AUTOINCREMENT, site_id integer, name text, url text, n_visits integer, total_elements integer, so_far_visit integer)')
# crea tabla de sub_secciones
c.execute('create table site_subsections (id integer PRIMARY KEY AUTOINCREMENT, site_section_id integer, name text, url text, n_visits integer, total_elements integer, so_far_visit integer)')

# ingresa mercado libre por default
c.execute("insert into sites values (1, 'Mercado Libre EC', 'https://www.mercadolibre.com.ec/', 0)")

conn.commit()
conn.close()