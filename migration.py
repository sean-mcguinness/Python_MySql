# Autor: Sean McGuinness
# Dieses Skript steht allen frei zur Verfuegung
# 2023

# Imports
import getpass
import mysql.connector

### Fuer Manuelles Setup ###
# Connection-Setup
man_db_host = None                          # Host eintragen (String) (default = localhost)
man_db_name = None                          # Name der DB eintragen (String)
man_db_user = None                          # Benutzername der DB eintragen (String)
man_db_password = None                      # Passwort des DB-Benutzers eintragen (String)
man_db_auth_plugin = None                   # Auth Plugin eintragen (String) (default = mysql_native_password)

# DB-Param-Setup
man_source_table = None                     # Quelle (Tabellename) eintragen (String)
man_source_attribute = None                 # Quellattribut (Attribut) eintragen (String)
man_sep_choice = None                       # Separator nachdem die Daten geteilt werden eintragen (String) (default = " ")
man_destination_houseNr_attribute = None    # Zielattribut der Hausnummer eintragen (String)

man_destination_table = None                # Optional: Zieltabelle Eintragen (String)
man_destination_street_attribute = None     # Optional: Zielattribut der Strasse eintragen (String) (default = source_table)

dict_manual_edit = {
                    "db_name": man_db_name,
                    "db_user": man_db_user,
                    "db_password": man_db_password,
                    "source_table": man_source_table,
                    "source_attribute": man_source_attribute,
                    "destination_houseNr_attribute": man_destination_houseNr_attribute
                    }


def menu(dict_manual_edit):
    do_menu_loop = True

    while do_menu_loop:
        answer = input("Skript manuell bearbeitet? [J/N] ")

        if answer.upper == "J":
            do_menu_loop = False

            def check_params():
                for variable in dict_manual_edit:
                    if variable is None:
                        print(f"Die Definition von {variable} Fehlt!")
                        exit

            check_params()
            db_connection = connect_db(**dict_manual_edit)
            split_street_houseNr_in_db(db_connection)

        elif answer.upper == "N":
            do_menu_loop = False
            db_connection = interactive_edit()

        else:
            print("Ungültige Eingabe!")

    connect_db(db_connection)

    # Commitet die Aenderungen
    db_connection.commit()


def interactive_edit():

    ia_db_host = input("Hostname (default = localhost): ")
    ia_db_name = input("Name der Datenbank: ")
    ia_db_user = input("Benutzername: ")
    ia_db_password = getpass()
    ia_db_auth_plugin = input("DB Auth Plugin (default = mysql_native_password): ")
    ia_source_table = input("Quelle (Tabelle): ")
    ia_source_attribute = input("Quelle (Attribut): ")
    ia_destination_houseNr_attribute = input("Ziel (Attribut): ")

    dict_interactive_edit = {
                                "db_name": ia_db_name,
                                "db_user": ia_db_user,
                                "db_password": ia_db_password,
                                "source_table": ia_source_table,
                                "source_attribute": ia_source_attribute,
                                "destination_houseNr_attribute": ia_destination_houseNr_attribute
                                }

    if ia_db_host != "":
        dict_interactive_edit["hostname"] = ia_db_host

    if ia_db_auth_plugin != "":
        dict_interactive_edit["db_auth_plugin"] = ia_db_auth_plugin

    ask_use_optional_args = input("Optionale Argumente verwenden? (Andere Zieltabelle, Zielattribut für Strasse) [J/N] ")

    if ask_use_optional_args.upper == "J":
        do_loop1 = True
        while do_loop1:
            ask_use_other_dest_tbl = input("Andere Zieltabelle? [J/N]? ")

            if ask_use_other_dest_tbl.upper == "J":
                do_loop1 = False
                ia_destination_table = input("Zieltabelle: ")
                dict_interactive_edit["destination_table"] = ia_destination_table

            elif ask_use_other_dest_tbl.upper == "N":
                do_loop1 = False

            else:
                print("Ungültige Eingabe!")

        ask_use_other_str_dest = input("Anderes Zielattribut für Strasse? [J/N] ")

        do_loop2 = True
        while do_loop2:
            if ask_use_other_str_dest.upper == "J":
                do_loop2 = False
                ia_destination_street_attribute = input("Zielattribut für Strasse: ")
                dict_interactive_edit["destination_street_attribute"] = ia_destination_street_attribute

            elif ask_use_other_str_dest.upper == "N":
                do_loop2 = False

            else:
                print("Ungültige Eingabe!")

    return dict_interactive_edit


def connect_db(db_name, db_user, db_password, db_host="localhost", db_auth_plugin="mysql_native_password"):
    print(f"Mit {db_name} verbinden... ", end="", flush=True)
    # Verbindungsaufbau zur MYSQL Datenbank
    db_connection = mysql.connector.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        database=db_name,
        auth_plugin=db_auth_plugin
    )
    print("Erfolgreich verbunden")

    return db_connection


# SELECT Statement um die benoetigten Daten aus der Tabelle zu ziehen
stm_select_tbl = f"SELECT {source_attribute} FROM {source_table};"
print(stm_select_tbl)


def split_street_houseNr_in_db(db_connection, source_table, source_attribute,
                               destination_table=None, destination_street_attribute=None,
                               destination_houseNr_attribute=None, sep_choice=" "):

    # Befehle durch mysql.connector an Datenbank senden und ausfuehren
    mein_cursor = db_connection.cursor(dictionary=True)
    mein_cursor.execute(stm_select_tbl)
    mein_resultat = mein_cursor.fetchall()

    # Liste wird mit den benoetigten Daten gefuellt
    splitAddr = []

    # Fuellt Liste mit Daten
    for i in range(len(mein_resultat)):
        splitAddr += mein_resultat[i][source_attribute].rsplit(sep_choice, 1)

    if destination_table is None and destination_street_attribute is None:
        # Generiert UPDATE Statements für jeden Datensatz
        n = 2
        for x in range(len(mein_resultat)):

            if x == 0:
                stm_update = f"UPDATE {source_table} SET {source_attribute}= \
                \"{splitAddr[x]}\", {destination_houseNr_attribute}=\"{splitAddr[x+1]}\" WHERE id={x+1};"

            else:
                stm_update = f"UPDATE {source_table} SET {source_attribute}= \
                \"{splitAddr[n]}\", {destination_houseNr_attribute}=\"{splitAddr[n+1]}\" WHERE id={x+1};"
                n += 2
            print(f"Wird ausgeführt: {stm_update}")

            mein_cursor.execute(stm_update)
