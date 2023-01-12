
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where, Comparison

# Ejemplos
stringa1 = 'UPDATE people SET status = "C" WHERE age > 25'
stringa2 = 'UPDATE people SET age = age + 3 WHERE status = "A"'
stringa3 = 'UPDATE people SET status = "C", name = "Carlo" WHERE age > 25' 
stringa4 = 'UPDATE people SET status = "C", age = age + 1, name = "Carlo"  WHERE age > 25'


def update(tokens):
  tabla = ""
  encontrar_where = False
  encontrar_set = False
  set_salida = ""
  is_inc = False
  conj_salida_interior = []
  establecer_parentesis_salida = ""
  encontrar_listaId = False
# iterar a través de todos los tokens para ubicar el nombre de la tabla y encontrar la condición WHERE,
# que se almacena en la matriz analizada.
# (es) PARSED -> ['status', '=', '"D"', 'OR', 'name', '<=', '"Carlo"', 'AND', 'name', '!=', '"Saretta"']
  for token in tokens:
    if isinstance(token, Identifier):
      tabla = token.value
    if token.value == "SET":
      encontrar_set = True
    if isinstance(token,IdentifierList):
      encontrar_listaId = True
      conj_salida_interior = convertir_multiples_condiciones_update(token)
    if isinstance(token,Comparison) and encontrar_set:
      encontrar_set = False
      establecer_parentesis_salida = convertir_una_sola_condicion_update(token)
    if isinstance(token, Where):
      encontrar_where = True
      salida = convertir_condicion_where(token)
  # Si es necesario actualizar más campos, construya la consulta de salida en consecuencia
  if encontrar_listaId:
    establecer_parentesis_salida = formato_salida_listaId(conj_salida_interior)
   


# Si los operadores lógicos estuvieran presentes en la condición where
  # luego utilícelos para construir la consulta final, de lo contrario
  # si fuera una condición simple, construye la consulta final con solo
  # el único selector presente.
  if isinstance(salida[0],LogicOperator):
    consulta_final = "db."+ tabla +".update(" + salida[-1].created_string + ", " + establecer_parentesis_salida + " )"
  else:
    parentesis_salida = convertir_condicion_a_mongo(salida) 
    consulta_final = "db." + tabla + ".update(" + str(parentesis_salida[0]) + ", " + establecer_parentesis_salida +  ")"
  return(consulta_final)


def formato_salida_listaId(conj_salida_interior):
  establecer_parentesis_salida = ""
  combinar= combinar_subconjunto_lista(conj_salida_interior)
  contador = 0
  for tipo, value in combinar.items():
    contador = contador + 1
    establecer_parentesis_salida = establecer_parentesis_salida + "{" + tipo + ": "
    for i, val in enumerate(value):
      establecer_parentesis_salida = establecer_parentesis_salida  +"{"+ val + "}"
      if i != len(value)-1:
        establecer_parentesis_salida = establecer_parentesis_salida + ", "
      else:
        establecer_parentesis_salida = establecer_parentesis_salida + "}"
    if contador != len(combinar.items()):
      establecer_parentesis_salida = establecer_parentesis_salida + ", "
  return establecer_parentesis_salida

def convertir_multiples_condiciones_update(token):
  salida = []
  for id in token:
    if isinstance(id,Comparison):
      salida.append(crear_conjunto_salida_para_listaId(id))
  return salida

def convertir_una_sola_condicion_update(token):#CASI ZANJADO
  is_inc = False
  set_salida = token.value.split(" ")
  if (set_salida[0] == set_salida[2]):
    establecer_operador = "$inc"
    is_inc = True
  else:
    establecer_operador = "$set"
  if is_inc:
    segundo_elemento = set_salida[4]
  else:
    segundo_elemento = set_salida[2]
  establecer_parentesis_salida = "{" + establecer_operador + ": {" + set_salida[0] + ": " + segundo_elemento + "}}"
  return establecer_parentesis_salida

def combinar_subconjunto_lista(list):
  diccionario = {}
  for item in list:
    if item.tipo not in diccionario:
      diccionario[item.tipo] = [item.value]
    else:
      diccionario[item.tipo].append(item.value)
  return diccionario

def crear_conjunto_salida_para_listaId(token):
  is_inc = False
  salida = ""
  set_salida = token.value.split(" ")#['DEDE' 'DEDEDE']
  if (set_salida[0] == set_salida[2]):
    establecer_operador = "$inc"
    is_inc = True
  else:
    establecer_operador = "$set"
  if is_inc:
    segundo_elemento = set_salida[4]#SET_SALIDA[0 Y 2]=AGE Y SET_SALIDA[4]=1
  else:
    segundo_elemento = set_salida[2]
  #output = "{ " + establecer_operador + ": {" + set_salida[0] + ": " + segundo_elemento + "}}"
  salida = SubSet(establecer_operador, str(set_salida[0] + ": " + segundo_elemento))
  return salida

class SubSet:#SUBCONJUNTO
  def __init__(self, tipo = None, value = None):
    self.tipo = tipo
    self.value = value
  def __str__(self):
      return (str(self.__class__) + ": " + str(self.__dict__))


# Una vez que se encuentra la condición where, iterar a través de todos sus tokens
# para almacenar el tipo de operador lógico en el diccionario posiciones_operador_logico
# y su posición dentro de la condición where.
# (es) posiciones_operador_logico -> {3: 'OR', 7: 'AND'}

def crear_posicion_operador(parsed):
  posiciones_operador_logico = {}
  for i, item in enumerate(parsed, start = 0):
    if item == "AND" or item == "OR":
      posiciones_operador_logico[i] = "{0}".format(item)
  return posiciones_operador_logico


# Cree el vector 2D list_where_2D, que contiene muchas listas dentro
# de tokens de cuántas subcondiciones está compuesto el dónde.
# (es) lista_where_2D -> [['status', '=', '"D"'], ['name', '<=', '"Carlo"'], ['name', '!=', '"Saretta"']]

def crear_lista_subcondiciones(posiciones_operador_logico, parsed):
  posicion_inicial = 0
  lista_where_2D = []
  for key, value in posiciones_operador_logico.items():
    lista_temp = parsed[posicion_inicial:key]
    lista_where_2D.append(lista_temp)
    posicion_inicial = key + 1
    # Nel momento in cui viene raggiunto l'ultimo operatore logico, 
    # deve essere costruita l'ultima sottolista di lista_where_2D prendendo 
    # tutti i token rimamente in parsed.
    if key == list(posiciones_operador_logico.items())[-1][0]: 
      lista_temp = parsed[posicion_inicial:len(parsed)]
      lista_where_2D.append(lista_temp)
  return lista_where_2D


# En este punto, las subcondiciones se traducen de sql a sintaxis mongodb,
# primero traduciendo el selector (que siempre es el elemento central de cualquier sublista)
# y luego almacenar el resultado final en el vector output_parenthesis.
# (es) output_parenthesis -> ['{ status: { $eq: "D" }}', '{ name: { $lte: "Carlo" }}', '{ name: { $ne: "Saretta" }}']
def convertir_subcondiciones_a_mongo(lista_where_2D):
  parentesis_salida = []
  for item in lista_where_2D:
    if item[1] == "=":
      selector = "$eq"
    elif item[1] == "!=":
      selector = "$ne"
    elif item[1] == ">":
      selector = "$gt"
    elif item[1] == ">=":
      selector = "$gte"
    elif item[1] == "<":
      selector = "$lt"
    elif item[1] == "<=":
      selector = "$lte"
    sub_salida = "{ "+ item[0] +": { "+selector+": " + item[2] + "}"
    parentesis_salida.append(sub_salida + "}")
  return parentesis_salida


# El vector prioridad_operador_logico se crea asignando a cada operador logico
# una prioridad diferente: de hecho, todos los AND deben realizarse primero
# de izquierda a derecha y luego todos los OR de izquierda a derecha.
# (es) prioridad_operador_logico -> [7, 3]
def crear_prioridad_operadores(posiciones_operador_logico):
  prioridad_operador_logico = []
  operadores_logicos = []
  for key, value in posiciones_operador_logico.items():
    if value == "AND":
      prioridad_operador_logico.append(key)
  for key, value in posiciones_operador_logico.items():
    if value == "OR":
      prioridad_operador_logico.append(key)


# Las posiciones, prioridades y valores ahora están fusionados (Y/O)
  # de todos los operadores lógicos en objetos LogicOperator,
  # que se agregan a la lista operadores_logicos.
  # (es) operadores_logicos[0] -> {'pos': 3, 'tipo': 'OR', 'prioridad': 1, 'left': None, 'right': None, 'created_string': None}
  for key, value in posiciones_operador_logico.items():
    op = LogicOperator()
    op.pos = key
    op.tipo = value
    op.prioridad = prioridad_operador_logico.index(key)
    operadores_logicos.append(op)
  return operadores_logicos


# Para cada sublista contenida en list_where_2D se crea un objeto Block,
# y agregado a la lista de bloques. Cada bloque tendrá como atributos el id/ubicación del bloque,
# el valor en sql (extraído de list_where_2D)
# y el valor en mongodb (extraído de output_parenthesis).
# (es) blocks[0] -> {'id': 0, 'valor_sql': ['status', '=', '"D"'], 'valor_mongo': '{ status: { $eq: "D" }}', 'mapeo': None}

def crear_blocks(lista_where_2D, parentesis_salida):
  blocks = []
  for i, item in enumerate(lista_where_2D, start = 0):
    block = Block(i, item, parentesis_salida[i])
    blocks.append(block) 
  return blocks


# Cada operador lógico en operadores_logicos se asigna a la subcondición izquierda (op.left)
# y con la subcondición derecha (op.right) según su posición
# relativo en la condición where.
# Las subcondiciones pueden ser bloques (en el caso de operadores con prioridad
# de mayor ejecución) o el resultado de otros operadores ejecutados previamente.
# (es) operadores_logicos[0] -> {'pos': 7, 'tipo': 'AND', 'prioridad': 0, 'left': <__main__.Block object at 0x7f12d8b5ee80>, 'right': <__main__.Block object at 0x7f12d8b5ee48>, 'created_string': None}
def mapear(operadores_logicos, blocks):
  for op in operadores_logicos:
    pos_block_rel = op.posicion//3
    id_block_izq = pos_block_rel - 1
    id_block_der = pos_block_rel
    for block in blocks:
      if block.id == id_block_izq and block.mapeo == None:
        block.mapeo = op
        op.izquierda = block
      elif block.id == id_block_izq and block.mapeo != None:
        op.izquierda = block.mapeo
      elif block.id == id_block_der and block.mapeo == None:
        block.mapeo = op
        op.derecha = block
      elif block.id == id_block_der and block.mapeo != None:
        op.derecha = block.mapeo

# Los operadores se traducen a mongoDB,
# comenzando desde aquellos con mayor prioridad y almacenando el resultado
# parcial en el atributo created_string del bloque. Operadores posteriores
# que tienen ese bloque recién ejecutado como mapear hacia la izquierda o hacia la derecha construirá
# incrementalmente el resultado a partir del valor de created_string.
# El resultado final de la traducción estará contenido en el atributo
# created_string del último bloque.
def ejecutar_operadores (operadores_logicos):
  ult_id_op_ejec = None
  for op in operadores_logicos:
    if isinstance(op.izquierda, Block) and isinstance(op.derecha, Block):
      valor_izquierdo = op.izquierda.valor_mongo
      valor_derecho = op.derecha.valor_mongo
      ult_id_op_ejec = op.posicion
    elif isinstance(op.izquierda, OperadorLogico) and isinstance(op.derecha, Block):
      valor_izquierdo = buscar(operadores_logicos,ult_id_op_ejec).cadena_creada
      valor_derecho = op.derecha.valor_mongo
      ult_id_op_ejec = op.posicion
    elif isinstance(op.izquierda, Block) and isinstance(op.derecha, OperadorLogico):
      valor_izquierdo = op.izquierda.valor_mongo
      valor_derecho = buscar(operadores_logicos,ult_id_op_ejec).cadena_creada
      ult_id_op_ejec = op.posicion
    elif isinstance(op.izquierda, OperadorLogico) and isinstance(op.derecha, OperadorLogico):
      valor_izquierdo = buscar(operadores_logicos,ult_id_op_ejec).cadena_creada
      valor_derecho = buscar(operadores_logicos,ult_id_op_ejec).cadena_creada
      ult_id_op_ejec = op.posicion
    op.cadena_creada  = "{$" + op.tipo.lower() + ": [" + str(valor_izquierdo) + ", " +  str(valor_derecho) + "]}"

def convertir_condicion_a_mongo(parsed):
  parentesis_salida = []
  selector = ""
  for item in parsed:
    if item == "=":
      selector = "$eq"
    elif item == "!=":
      selector = "$ne"
    elif item == ">":
      selector = "$gt"
    elif item == ">=":
      selector = "$gte"
    elif item == "<":
      selector = "$lt"
    elif item == "<=":
      selector = "$lte"
  sub_salida = "{"+ parsed[0] +": {"+selector+": " + parsed[2] + " }"
  parentesis_salida.append(sub_salida + "}")
  return parentesis_salida

def convertir_condicion_where(token):
  parsed = token.value.split(" ")
  where = "WHERE"
  if where in parsed: parsed.remove(where)
  #parsed.remove("WHERE")
  posiciones_operador_logico = crear_posicion_operador(parsed)
  if posiciones_operador_logico:
    lista_where_2D = crear_lista_subcondiciones(posiciones_operador_logico, parsed)
    parentesis_salida = convertir_subcondiciones_a_mongo(lista_where_2D)
    operadores_logicos = crear_prioridad_operadores(posiciones_operador_logico)
    blocks = crear_blocks(lista_where_2D, parentesis_salida)
   # Gli operatori logici vengono ordinati in base alla loro priorità di esecuzione.
    operadores_logicos.sort(key=lambda x: x.prioridad, reverse=False)
    mapear(operadores_logicos,blocks)
    execute_ops(operadores_logicos)
    salida = operadores_logicos
  else:
    salida = parsed
  return salida


# Clase del operador lógico AND/OR. El pos indica la posición dentro de la condición where,
# (y por lo tanto actúa como id), el tipo indica el tipo AND/OR, la prioridad el orden de ejecución, izquierda y derecha
# las subcondiciones izquierda y derecha, mientras que el resultado se almacena en created_string
# de su traducción a la sintaxis mongoDB.
class LogicOperator:
    def __init__(self, pos = None, tipo = None, prioridad = None, left = None, right = None, created_string = None):
      self.pos = pos
      self.tipo = tipo
      self.prioridad = prioridad
      self.left = left
      self.right = right
      self.created_string = created_string
    def __str__(self):
      return (str(self.__class__) + ": " + str(self.__dict__))



# La clase de bloque indica una subcondición que precede o sigue a un operador lógico
# en la condición inicial donde. Por lo tanto, se caracteriza por una identificación (ubicación del bloque),
# da una traducción en sql (valor_sql) y otra en mognodb (valor_mongo).
# El atributo mapeo se usa para mapear un operador posterior a uno anterior
# que ya ha mapeado ese bloque.
class Block:
  def __init__(self, id, valor_sql = None, valor_mongo = None, mapeo = None):
    self.id = id
    self.valor_sql = valor_sql
    self.valor_mongo = valor_mongo
    self.mapeo = mapeo
  def __str__(self):
    return (str(self.__class__) + ": " + str(self.__dict__))
