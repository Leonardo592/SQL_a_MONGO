from ply import lex, yacc
import tkinter as tk
import tkinter.ttk as ttk
import sqlparse
import delete as dl
import insert as ins
import update as upd

def Convertir(label3, cadena):
    mongo_query = ""
    query = cadena.get()
    formatted = sqlparse.format(query, keyword_case="upper")#transforma mayuscula
    parsed = sqlparse.parse(formatted)  #analiza la primera cadena
    token_list = parsed[0]  #extraer la primera consulta (si hay más de una en la cadena)
    tokens = token_list.tokens  #extrae todas las palabras de la consulta y las clasifica según su tipo
    query_type = tokens[0].value 
    if query_type == "DELETE":
        print("Es una consulta delete...")
        mongo_query = dl.delete(tokens)
    elif query_type == "INSERT":
        print("Es una consulta insert...")
        mongo_query = ins.insert(tokens)
    elif query_type == "UPDATE":
        print("Es una consulta update...")
        mongo_query = upd.update(tokens)
    
    print("La consulta equivalente en mongo es: ", mongo_query)
    label3.delete('1.0', tk.END) #borrar el cuadro de texto
    label3.insert(tk.END, mongo_query)

class SqlLexer(object):

    reserved = {
       'insert' : 'INSERT', 
       'into'   : 'INTO',
       'delete' : 'DELETE',
       'update' : 'UPDATE',
       'select' : 'SELECT',
       'from'   : 'FROM',
       'where'  : 'WHERE',
       'set'    : 'SET',
       'order'  : 'ORDER',
       'by'     : 'BY',
       'values' : 'VALUES',
       'and'    : 'AND',
       'or'     : 'OR',
       'not'    : 'NOT',
    } 
    
    tokens = ['NUMERO',
              'ID', 
              'STRING',
              'COMA',      'SEMI',
              'SUMA',       'RESTA',
              'MULT',      'DIVIDE',
              'PARENI',     'PAREND',
              'MYQ',         'MYE',
              'MNQ',         'MNE',
              'EQ',         'NE', 
              ] + list(reserved.values())
    
    def t_NUMERO(self, t):
        r'\d+'
        t.value = int(t.value)    
        return t
    
    def t_ID(self, t):
        r'[a-zA-Z_][a-zA-Z_0-9]*'
        t.type = SqlLexer.reserved.get(t.value,'ID')    
        t.value = t.value.lower()
        return t
    
    def t_STRING(self, t):
        '(?:"(?:[^"\\n\\r\\\\]|(?:"")|(?:\\\\x[0-9a-fA-F]+)|(?:\\\\.))*")|(?:\'(?:[^\'\\n\\r\\\\]|(?:\'\')|(?:\\\\x[0-9a-fA-F]+)|(?:\\\\.))*\')'
        t.value = eval(t.value) 
        return t
        
    def t_newline(self, t):
        r'\n+'
        t.lexer.lineno += len(t.value)
    
    t_ignore  = ' \t'
    
    t_COMA   = r'\,'
    t_SEMI    = r';'
    t_SUMA    = r'\+'
    t_RESTA   = r'-'
    t_MULT   = r'\*'
    t_DIVIDE  = r'/'
    t_PARENI  = r'\('
    t_PAREND  = r'\)'
    t_MYQ      = r'>'
    t_MYE      = r'>='
    t_MNQ      = r'<'
    t_MNE      = r'<='
    t_EQ      = r'='
    t_NE      = r'!='
    
    def t_error(self, t):
        raise TypeError("Texto desconocido '%s'" % (t.value,))

    def build(self, **kwargs):
        self.lexer = lex.lex(module=self, **kwargs)
        return self.lexer

    def test(self):
        while True:
            text = input("sql> ").strip()
            if text.lower() == "salir":
                break
            self.lexer.input(text)
            while True:
                tok = self.lexer.token()
                if not tok: 
                    break
                print (tok)
        
        
class SqlParser(object):
    
    tokens = SqlLexer.tokens
    
    precedence = (
        ('left', 'OR'),
        ('left', 'AND'),
        ('left', 'NOT'),
        ('left', 'EQ', 'NE', 'MNQ', 'MYQ', 'MNE', 'MYE'),
        ('left', 'SUMA', 'RESTA'),
        ('left', 'MULT', 'DIVIDE'),
        )
    
    def p_lista_declaracion(self, p):
        """
        lista_declaracion : declaracion
                       | lista_declaracion declaracion
        """
        if len(p) == 2:
            p[0] = [p[1]]
        else:
            p[0] = p[1] + [p[2]]
            
    def p_declaracion(self, p):
        """
        declaracion : declaracion_insert
                  | declaracion_select
                  | declaracion_delete
                  | declaracion_update
        """
        p[0] = p[1]
    
    def p_declaracion_insert(self, p):
        """
        declaracion_insert : INSERT INTO ID PARENI lista_id PAREND VALUES PARENI lista_expr PAREND SEMI
        """

    def p_declaracion_delete(self, p):
        """
        declaracion_delete :  DELETE FROM ID
                            | DELETE MULT FROM ID 
                            | DELETE FROM ID WHERE columna_seleccion EQ atom SEMI
                            | DELETE FROM ID WHERE columna_seleccion EQ atom AND columna_seleccion EQ atom SEMI
                            | DELETE FROM ID WHERE columna_seleccion EQ atom AND columna_seleccion EQ atom AND columna_seleccion EQ atom SEMI
                            | DELETE FROM ID WHERE columna_seleccion EQ atom OR columna_seleccion EQ atom SEMI
                            | DELETE FROM ID WHERE columna_seleccion EQ atom OR columna_seleccion EQ atom OR columna_seleccion EQ atom SEMI
                            | DELETE FROM ID WHERE columna_seleccion EQ atom AND columna_seleccion EQ atom OR columna_seleccion EQ atom SEMI
                            | DELETE FROM ID WHERE columna_seleccion EQ atom OR columna_seleccion EQ atom AND columna_seleccion EQ atom SEMI
        """

    def p_declaracion_update(self, p):
        """
        declaracion_update : UPDATE ID SET columna_seleccion EQ atom WHERE columna_seleccion MYQ atom SEMI
                            | UPDATE ID SET columna_seleccion EQ atom WHERE columna_seleccion MYE atom SEMI
                            | UPDATE ID SET columna_seleccion EQ atom WHERE columna_seleccion MNQ atom SEMI
                            | UPDATE ID SET columna_seleccion EQ atom WHERE columna_seleccion MNE atom SEMI
                            | UPDATE ID SET columna_seleccion EQ atom WHERE columna_seleccion EQ atom SEMI
                            | UPDATE ID SET columna_seleccion EQ columna_seleccion SUMA atom WHERE columna_seleccion EQ atom SEMI
                            | UPDATE ID SET columna_seleccion EQ columna_seleccion RESTA atom WHERE columna_seleccion EQ atom SEMI
                            | UPDATE ID SET columna_seleccion EQ columna_seleccion MULT atom WHERE columna_seleccion EQ atom SEMI
                            | UPDATE ID SET columna_seleccion EQ columna_seleccion DIVIDE atom WHERE columna_seleccion EQ atom SEMI
                            | UPDATE ID SET columna_seleccion EQ atom COMA columna_seleccion EQ atom WHERE columna_seleccion MYQ atom SEMI
                            | UPDATE ID SET columna_seleccion EQ atom COMA columna_seleccion EQ atom WHERE columna_seleccion MYE atom SEMI
                            | UPDATE ID SET columna_seleccion EQ atom COMA columna_seleccion EQ atom WHERE columna_seleccion MNQ atom SEMI
                            | UPDATE ID SET columna_seleccion EQ atom COMA columna_seleccion EQ atom WHERE columna_seleccion MNE atom SEMI
                            | UPDATE ID SET columna_seleccion EQ atom COMA columna_seleccion EQ atom WHERE columna_seleccion EQ atom SEMI
                            | UPDATE ID SET columna_seleccion EQ atom COMA columna_seleccion EQ atom WHERE columna_seleccion NE atom SEMI
                            | UPDATE ID SET columna_seleccion EQ atom COMA columna_seleccion EQ columna_seleccion SUMA NUMERO COMA columna_seleccion EQ atom WHERE columna_seleccion MYQ NUMERO SEMI
                            | UPDATE ID SET columna_seleccion EQ atom COMA columna_seleccion EQ columna_seleccion SUMA NUMERO COMA columna_seleccion EQ atom WHERE columna_seleccion MYE NUMERO SEMI
                            | UPDATE ID SET columna_seleccion EQ atom COMA columna_seleccion EQ columna_seleccion SUMA NUMERO COMA columna_seleccion EQ atom WHERE columna_seleccion MNQ NUMERO SEMI
                            | UPDATE ID SET columna_seleccion EQ atom COMA columna_seleccion EQ columna_seleccion SUMA NUMERO COMA columna_seleccion EQ atom WHERE columna_seleccion MNE NUMERO SEMI
                            | UPDATE ID SET columna_seleccion EQ atom COMA columna_seleccion EQ columna_seleccion SUMA NUMERO COMA columna_seleccion EQ atom WHERE columna_seleccion EQ NUMERO SEMI
                            | UPDATE ID SET columna_seleccion EQ atom COMA columna_seleccion EQ columna_seleccion SUMA NUMERO COMA columna_seleccion EQ atom WHERE columna_seleccion NE NUMERO SEMI
        """

    def p_declaracion_select(self, p):
        """
        declaracion_select : SELECT columna_seleccion FROM ID opr_clausula_where opr_clausula_orderby
        """
        
    def p_columna_seleccion(self, p):
        """
        columna_seleccion : MULT
                       | lista_id
        """
        
    def p_clausula_where(self, p):
        """
        opr_clausula_where : WHERE condicion_buscada
                         |
        """
            
    def p_condicion_buscada(self, p):
        """
        condicion_buscada : condicion_buscada OR condicion_buscada
                         | condicion_buscada AND condicion_buscada
                         | NOT condicion_buscada
                         | PARENI condicion_buscada PAREND
                         | predicado
        """
            
    def p_predicado(self, p):
        """
        predicado : comparacion_predicado
        """
        
    def p_comparacion_predicado(self, p):
        """
        comparacion_predicado : exp_escalar EQ exp_escalar
                             | exp_escalar NE exp_escalar
                             | exp_escalar MNQ exp_escalar
                             | exp_escalar MNE exp_escalar
                             | exp_escalar MYQ exp_escalar
                             | exp_escalar MYE exp_escalar
        """
        
    def p_exp_escalar(self, p):
        """
        exp_escalar : exp_escalar SUMA exp_escalar
                   | exp_escalar RESTA exp_escalar
                   | exp_escalar MULT exp_escalar
                   | exp_escalar DIVIDE exp_escalar
                   | atom
                   | PARENI exp_escalar PAREND
        """
        
    def p_atom(self, p):
        """
        atom : NUMERO
             | ID
             | STRING
        """
            
    def p_opt_clausula_orderby(self, p):
        """
        opr_clausula_orderby : ORDER BY ID
                           |
        """
        
    def p_lista_id(self, p):
        """
        lista_id : ID
                | lista_id COMA ID
        """

    def p_lista_expr(self, p):
        """
        lista_expr : expr
                  | lista_expr COMA expr
        """

    def p_expr(self, p):
        """
        expr : expr SUMA term
             | expr RESTA term
             | term
             | ID
             | STRING
        """
            
    def p_term(self, p):
        """
        term : term MULT factor
             | term DIVIDE factor
             | factor
        """

    def p_factor(self, p):
        """
        factor : NUMERO
               | PARENI expr PAREND
        """
    
    def p_error(self, p):
        print ( "Error de sintaxis en la entrada" ) 
    
    def build(self, **kwargs):
        self.parser = yacc.yacc(module=self, **kwargs)
        return self.parser

    def test(self):
        lexer = SqlLexer().build()
        while True:
            text = input("sql> ").strip()
            if text.lower() == "salir":
                break
            window = tk.Tk()
            window.title("SQL A MONGO")
            window.minsize(50, 50)
            
            style = ttk.Style(window)
            window.tk.call('source','azure/azure.tcl')
            style.theme_use('azure')
            style.configure("Accentbutton", foreground='white')
            label1 = ttk.Label(
                window,
                text="Consulta SQL",
                compound="center",
                font="arial 15")
            label1.grid(column=0, row=0, padx=20,pady=5)

            label2 = ttk.Label(
                window,
                text="Consulta equivalente en MQL",
                compound="center",
                font="arial 15")
            label2.grid(column=0, row=2, padx=5,ipady=10)

            button = ttk.Button(
                window,
                text="Cerrar",
                style="Accentbutton",
                command=window.destroy)
            button.grid(column=0,row=4,pady=15)

            label3 = tk.Text(window, width=100, height=10)
            label3.grid(column=0, row=3, pady=5)
            label3.insert(tk.END,"")
            cadena = tk.StringVar(value=text)
            cadenaIngresada = ttk.Entry(window, width=100, textvariable=cadena)
            cadenaIngresada.grid(column=0, row=1, padx=20,pady=10)
            if text:
                result = self.parser.parse(text, lexer=lexer)
                if result is not None:
                    Convertir(label3, cadena) 
                else: 
                    mongo_query = "ERROR DE SINTAXIS"
                    label3.delete('1.0', tk.END) 
                    label3.insert(tk.END, mongo_query)
                    
            window.mainloop()
        

def unittest_lexer():
    l = SqlLexer()
    l.build()
    l.test()
        
def unittest_parser():
    p = SqlParser()
    p.build()
    p.test()
    
if __name__ == "__main__":    
    unittest_parser()
    
                