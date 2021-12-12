

python -m elabs.bitpool.BitPool reset 
python -m elabs.bitpool.BitPool create 
python -m elabs.bitpool.BitPool run 

python -m elabs.bitpool.BitPool get_latest ftx swap 1 ALT-PERP --num=10

python -m elabs.app.core.registry get_symbol_names --exchange=ftx --tt=swap --url=http://172.16.10.253:17027
