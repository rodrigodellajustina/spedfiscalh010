import pandas as pd
import os.path

class Sped:
    _val_entrada = 0
    _val_saida   = 0
    _dir  = ""
    _path = ""
    _h010  = ""
    _h010r = ""
    _proc1 = ""
    _proc2 = ""
    _proc3 = ""
    _procrevision = ""
    _proc  = ""


    def __init__(self, val_entrada, val_saida, directory, file):
        self._val_entrada = val_entrada
        self._val_saida   = val_saida
        self._dir = directory
        self._path = file
        self._dir_exist = False
        self.condir()

    def ratear(self, valorcelula):
        percentual = (valorcelula * 100) / self._val_entrada
        valorcelula = (self._val_saida * (valorcelula * 100) / self._val_entrada) / 100
        return str(round(valorcelula,2))

    def condir(self):
        if self._dir != "":
            self._dir   = "/"+(self._dir + "/" + self._path)
            self._h010  = "/tmp/" + "h010.txt"
            self._h010r = "/tmp/" + "h010r.txt"
            self._proc1 = "/tmp/" + "proc1.txt"
            self._proc2 = "/tmp/" + "proc2.txt"
            self._proc3 = "/tmp/" + "proc3.txt"
            self._procrevision = "/tmp/" + "procrevision.txt"
            self._proc   =  "/tmp/" + self._path+".proc"
        else:
            self._dir =  self._path
            self._h010 =  "h010.txt"
            self._h010r = "h010r.txt"
            self._proc1 = "proc1.txt"
            self._proc2 = "proc2.txt"
            self._proc3 = "proc3.txt"
            self._procrevision = "procrevision.txt"
            self._proc = self._path + ".proc"

        if not os.path.exists(self._dir):
            print("O arquivo {0} não foi encontrado, faça upload na pasta tmp".format(self._dir))
            self._dir_exist = False
        else:
            self._dir_exist = True

    def processarh010(self, sped):
        if sped._dir_exist:
            import pandas as pd
            print("Valor de Entrada do Sped " + str(sped._val_entrada))
            print("Valor de Saída do Sped " + str(sped._val_saida))
            days_file = open(sped._dir, 'r', encoding='windows-1252')
            days = days_file.read()

            # Processamento
            lt = days.split("\n")
            df = pd.DataFrame(lt, columns=["valor"])
            x = df.query('valor.str.contains("H010")', engine="python")
            df2 = pd.DataFrame()
            df2 = x['valor'].str.split('|', expand=True)
            df2.columns = ['a1', 'a2', 'product', 'unidade', 'qtd', 'preco', 'total', 'n1', 'n2', 'descricao', 'ncm',
                           'n3', 'n4']
            df2['preco'] = df2['preco'].str.replace(',', '.')
            df2['total'] = df2['total'].str.replace(',', '.')
            df2['qtd'] = df2['qtd'].str.replace(',', '.')
            df2['qtd'] = pd.to_numeric(df2["qtd"])
            df2['preco'] = pd.to_numeric(df2["preco"])
            df2['total'] = pd.to_numeric(df2["total"])
            df2["preco"] = round((sped._val_saida * (df2["preco"] * 100) / sped._val_entrada) / 100, 2)
            df2['total'] = round(df2['preco'] * df2['qtd'], 2)

            total = round(df2["total"].sum(), 2)

            while (total != sped._val_saida):
                print("Total sem diferença ", total)
                if sped._val_saida > total:
                    diff = round(sped._val_saida - total, 2)
                else:
                    diff = round(total - sped._val_saida, 2)
                print("Caculando Diferença ", diff)
                positionfirst = df2[df2['preco'] > diff].index.values[0]
                if sped._val_saida > total:
                    df2.at[positionfirst, "preco"] = df2["preco"][positionfirst] + diff
                else:
                    df2.at[positionfirst, "preco"] = df2["preco"][positionfirst] - diff
                df2['total'] = round(df2['preco'] * df2['qtd'], 2)
                total = round(df2["total"].sum(), 2)
                print("Total após a Diferença ", total)

            df2['qtd'] = df2['qtd'].astype(str)
            df2['qtd'] = df2['qtd'].str.replace(".", ",", regex=False)
            df2['preco'] = df2['preco'].round(2)
            df2['preco'] = df2['preco'].astype(str)
            df2['preco'] = df2['preco'].str.replace(".", ",", regex=False)
            df2['total'] = df2['total'].astype(str)
            df2['total'] = df2['total'].str.replace(".", ",", regex=False)
            df2["export"] = "|" + df2['a2'] + "|" + df2['product'] + "|" + df2['unidade'] + "|" + df2['qtd'] + "|" + \
                            df2['preco'] + "|" + df2['total'] + "|0|" + "|" + df2['descricao'].str.replace('"',
                                                                                                           "'") + "|" + \
                            df2['ncm'] + "|0|"

            # Gerando arquivos temporários
            df2["export"].to_csv(sped._h010, header=None, index=False)

            with open(sped._h010, 'r', encoding="utf-8", errors="ignore") as file:
                filedata = file.read()

            filedata = filedata.replace('"', '')
            filedata = filedata.replace("'", '"')

            with open(sped._h010r, 'w', encoding="windows-1252") as file:
                file.write(filedata)

            ##Formata o Arquivo...

            with open(sped._dir, 'r', encoding='windows-1252') as fp:
                # read an store all lines into list
                lines = fp.readlines()

            with open(sped._h010r, 'r', encoding='windows-1252') as fp2:
                # read an store all lines into list
                lines2 = fp2.readlines()

            # Arquivo 01
            with open(sped._proc1, 'w', encoding='windows-1252') as fp:
                # iterate each line
                for number, line in enumerate(lines):
                    if "|H005|" in line:
                        novovalor = str(sped._val_saida)
                        novovalor = novovalor.replace(".", ",")
                        fp.write('|H005|31122021|{0}|01|\n'.format(novovalor))
                    if "|H010|" not in line and '|H005|' not in line:
                        fp.write(line)
                    else:
                        break

            # Arquivo 02
            with open(sped._proc2, 'w', encoding='windows-1252') as fp:
                # iterate each line
                for number, line in enumerate(lines2):
                    if "|H010|" in line:
                        fp.write(line)

            # Arquivo 03
            with open(sped._proc3, 'w', encoding='windows-1252') as fp:
                # iterate each line
                liga = False
                for number, line in enumerate(lines):
                    if "|H990|" in line:
                        liga = True

                    if liga:
                        fp.write(line)

            # Merge com os Arquivos
            data = data2 = data3 = ""

            with open(sped._proc1, encoding='windows-1252') as fp:
                data1 = fp.read()

            with open(sped._proc2, encoding='windows-1252') as fp:
                data2 = fp.read()

            # Reading data from file2
            with open(sped._proc3, encoding='windows-1252') as fp:
                data3 = fp.read()

            data1 += data2
            data1 += data3

            with open(sped._procrevision, 'w', encoding='windows-1252') as fp:
                fp.write(data1)

            with open(sped._procrevision, 'r', encoding='windows-1252') as fp:
                # read an store all lines into list
                lines = fp.readlines()

            with open(sped._proc, 'w', encoding='windows-1252') as fp:
                for number, line in enumerate(lines):
                    if "|" in line:
                        fp.write(line)

            if os.path.exists(sped._proc):
                print("Arquivo gerado com sucesso {0}".format(sped._proc))

