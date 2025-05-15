pygettext3 -d pybackup -o locales/pybackup.pot pybackup_gui.py
msgmerge --update locales/en/LC_MESSAGES/pybackup.po locales/pybackup.pot
msgmerge --update locales/fr/LC_MESSAGES/pybackup.po locales/pybackup.pot