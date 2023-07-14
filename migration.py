# Autor: Sean McGuinness
# Dieses Skript steht allen frei zur Verfuegung
# 2023

# Imports
import getpass                                  # Passworteingabe im interaktiven Modus
import mysql.connector                          # MySql Schnittstelle

### Fuer manuelles Setup ###
# Connection-Setup
man_db_host = "localhost"                       # Host eintragen (String) (default = localhost)
man_db_name = None                              # Name der DB eintragen (String)
man_db_user = None                              # Benutzername der DB eintragen (String)
man_db_password = None                          # Passwort des DB-Benutzers eintragen (String)
man_db_auth_plugin = "mysql_native_password"    # Auth Plugin eintragen (String) (default = mysql_native_password)

# DB-Param-Setup
man_source_table = None                         # Quelle (Tabellename) eintragen (String)
man_source_attribute = None                     # Quellattribut (Attribut) eintragen (String)
man_separator = " "                            # Separator nachdem die Daten geteilt werden eintragen (String) (default = " ")
man_destination_houseNr_attribute = None        # Zielattribut der Hausnummer eintragen (String)

man_destination_table = None                    # Optional: Zieltabelle Eintragen (String)
man_destination_street_attribute = None         # Optional: Zielattribut der Strasse eintragen (String) (default = source_table)

# Dictionary wird beim Funktionsaufruf entpackt
dict_connection_manual_edit = {
                    "db_name": man_db_name,
                    "db_user": man_db_user,
                    "db_password": man_db_password,
                    "db_host": man_db_host,
                    "db_auth_plugin": man_db_auth_plugin,
                    }

dict_db_manual_edit = {
                    "source_table": man_source_table,
                    "source_attribute": man_source_attribute,
                    "separator": man_separator,
                    "destination_houseNr_attribute": man_destination_houseNr_attribute,
                    "destination_table": man_destination_table,
                    "destination_street_attribute": man_destination_street_attribute
                    }

dict_manual_edit = {
                    "connection": dict_connection_manual_edit,
                    "db": dict_db_manual_edit
                    }


def main(dict_manual_edit):

    do_menu_loop = True

    while do_menu_loop:
        answer = input("Skript manuell bearbeitet? [J/N] ")

        if answer.upper() == 'J':
            # Prueft die Manuellen anpassungen im Skript(Sind alle Pflichtvariablen definiert?)
            def check_params(dict_manual_edit):
                for nested_dicts in dict_manual_edit.values():
                    for key, value in nested_dicts.items():
                        if value is None:
                            print(f"Die Definition von {key} fehlt!")
                            exit()

            check_params(dict_manual_edit)
            # Verbindet mit der DB und gibt die Connection zurueck
            db_connection = connect_db(**dict_manual_edit["connection"])
            # Teilt die von der DB erhaltenen Daten auf und fuehrt UPDATE durch
            split_street_houseNr_in_db(db_connection, **dict_manual_edit["db"])
            do_menu_loop = False

        elif answer.upper() == 'N':
            # "Aktiviert" den interaktiven Modus (Sammelt benoetigte user Inputs und gibt ein dictionary zurueck)
            ia_dict = interactive_edit()
            db_connection = connect_db(**ia_dict["connection"])
            split_street_houseNr_in_db(db_connection, **ia_dict["db"])
            do_menu_loop = False
        else:
            print("Ungültige Eingabe!")

    # Commitet die Aenderungen
    db_connection.commit()


def interactive_edit():

    ia_db_host = input("Hostname (default = localhost): ")
    ia_db_name = input("Name der Datenbank: ")
    ia_db_user = input("Benutzername: ")
    ia_db_password = getpass.getpass("Passwort: ")
    ia_db_auth_plugin = input("DB Auth Plugin (default = mysql_native_password): ")
    ia_source_table = input("Quelle (Tabelle): ")
    ia_source_attribute = input("Quelle (Attribut): ")
    ia_destination_houseNr_attribute = input("Ziel (Attribut): ")

    dict_connection_ia_edit = {
                        "db_host": "localhost",
                        "db_name": ia_db_name,
                        "db_user": ia_db_user,
                        "db_password": ia_db_password,
                        "db_auth_plugin": "mysql_native_password",
                        }

    dict_db_ia_edit = {
                       "source_table": ia_source_table,
                       "source_attribute": ia_source_attribute,
                       "destination_houseNr_attribute": ia_destination_houseNr_attribute,
                       "destination_table": ia_source_table,
                       "destination_street_attribute": ia_source_attribute,
                       "separator": None
                       }

    if ia_db_host != "":
        dict_connection_ia_edit["hostname"] = ia_db_host

    if ia_db_auth_plugin != "":
        dict_connection_ia_edit["db_auth_plugin"] = ia_db_auth_plugin

    ask_use_optional_args = input("Optionale Argumente verwenden? (Andere Zieltabelle, Zielattribut für Strasse) [J/N] ")

    if ask_use_optional_args.upper() == "J":
        do_loop1 = True
        while do_loop1:
            ask_use_other_dest_tbl = input("Andere Zieltabelle? [J/N]? ")

            if ask_use_other_dest_tbl.upper() == "J":
                do_loop1 = False
                ia_destination_table = input("Zieltabelle: ")
                dict_db_ia_edit["destination_table"] = ia_destination_table
            elif ask_use_other_dest_tbl.upper() == "N":
                do_loop1 = False
            else:
                print("Ungültige Eingabe!")

        ask_use_other_str_dest = input("Anderes Zielattribut für Strasse? [J/N] ")

        do_loop2 = True
        while do_loop2:
            if ask_use_other_str_dest.upper() == "J":
                do_loop2 = False
                ia_destination_street_attribute = input("Zielattribut für Strasse: ")
                dict_db_ia_edit["destination_street_attribute"] = ia_destination_street_attribute
            elif ask_use_other_str_dest.upper() == "N":
                do_loop2 = False
            else:
                print("Ungültige Eingabe!")
        ask_use_separator = input("Anderen Separator als default verwenden? (default = " ") [J/N]")

        do_loop3 = True
        while do_loop3:
            if ask_use_separator.upper() == "J":
                do_loop3 = False
                ia_separator = input("Separator: ")
                dict_db_ia_edit["separator"] = ia_separator
            elif ask_use_separator.upper() == "N":
                do_loop3 = False
                pass
            else:
                print("Ungültige Eingabe!")

    dict_ia_edit = {"connection": dict_connection_ia_edit, "db": dict_db_ia_edit}
    return dict_ia_edit


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


def split_street_houseNr_in_db(db_connection,
                               source_table, source_attribute,
                               destination_table=None, destination_street_attribute=None, destination_houseNr_attribute=None,
                               separator=" "):

    # SELECT Statement um die benoetigten Daten aus der Tabelle zu ziehen
    stm_select_tbl = f"SELECT {source_attribute} FROM {source_table};"
    print(stm_select_tbl)

    # Befehle durch mysql.connector an Datenbank senden und ausfuehren
    mein_cursor = db_connection.cursor(dictionary=True)
    mein_cursor.execute(stm_select_tbl)
    mein_resultat = mein_cursor.fetchall()

    # Liste wird mit den benoetigten Daten gefuellt
    street_houseNr = []

    # Fuellt Liste mit Daten
    for i in range(len(mein_resultat)):
        street_houseNr += mein_resultat[i][source_attribute].rsplit(" " if separator is None else separator, 1)

    # Teilt Daten in 2 Listen
    dict_split_street_houseNr = split_street_houseNr_in_string(street_houseNr)

    # Generiert UPDATE Statements fuer jeden Datensatz
    for x in range(0, len(mein_resultat)):
        stm_update = f"UPDATE {destination_table} \
                        SET {destination_street_attribute}= \
                        \"{dict_split_street_houseNr['streets'][x]}\", \
                        {destination_houseNr_attribute}= \"{dict_split_street_houseNr['houseNrs'][x]}\" \
                        WHERE id={x+1};"

        print(f"Wird ausgefuehrt: {stm_update}")
        mein_cursor.execute(stm_update)


def split_street_houseNr_in_string(street_houseNr):
    street = [street_houseNr[index] for index in range(0, len(street_houseNr), 2)]
    houseNr = [street_houseNr[index] for index in range(1, len(street_houseNr), 2)]

    dict_split_street_houseNr = {"streets": street, "houseNrs": houseNr}

    return dict_split_street_houseNr


# Startet das Skript
main(dict_manual_edit)
