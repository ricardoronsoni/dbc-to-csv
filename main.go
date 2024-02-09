package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	godbf "github.com/LindsayBradford/go-dbf/godbf"
)

var (
	arquivosDbc = []string{}
	arquivosDbf = []string{}
)

func main() {
	deletarDbfCsv()
	listarArquivosDbc()

	if len(arquivosDbc) > 0 {
		dbcParaDbf()
		dbfParacsv()
	}

	fmt.Println("Processo finalizado.")
}

// deletarDbfCsv exclui todos os arquivos .DBF e .CSV antes de iniciar as transformações
func deletarDbfCsv() {
	filepath.Walk("./arquivosDbc", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Fatal("Erro ao deletar os arquivos antes de iniciar o programa:", err)
		}
		if !info.IsDir() && (strings.HasSuffix(strings.ToUpper(info.Name()), ".DBF") || strings.HasSuffix(strings.ToUpper(info.Name()), ".CSV")) {
			err := os.Remove(path)
			if err != nil {
				log.Fatal("Erro ao deletar os arquivos antes de iniciar o programa:", err)
			}
		}
		return nil
	})
}

// listarArquivosDbc irá listar todos os arquivos DBC no diretório ./arquivosDBC
func listarArquivosDbc() {
	filepath.Walk("./arquivosDbc", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Fatal("Erro ao listar os arquivos CSV:", err)
		}
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".dbc") {
			relativePath, err := filepath.Rel("./arquivosDbc", path)
			if err != nil {
				log.Fatal("Erro ao listar os arquivos CSV:", err)
			}
			arquivosDbc = append(arquivosDbc, relativePath)
		}
		return nil
	})

	fmt.Println("Foi(ram) localizado(s) " + fmt.Sprint(len(arquivosDbc)) + " arquivo(s) DBC.")
}

// dbcParaDbf transforma os arquivos DBC em DBF
func dbcParaDbf() {
	for _, nomeArquivo := range arquivosDbc {
		arquivoDbf := strings.TrimSuffix(nomeArquivo, ".dbc") + ".dbf"
		arquivosDbf = append(arquivosDbf, arquivoDbf)

		//usa comando shell para acionar o programa em C na pasta ./blast-dbf
		cmd := exec.Command("/bin/sh", "-c", "cd blast-dbf/; ./blast-dbf ../arquivosDbc/"+nomeArquivo+" ../arquivosDbc/"+arquivoDbf)
		err := cmd.Run()
		if err != nil {
			log.Fatal("Erro ao transformar o arquivo DBF em DBC:", err)
		}
		fmt.Println(nomeArquivo + ": Arquivo convertido para DBF.")
	}
}

// dbfParacsv transforma os arquivos DBF em CSV utilizando a biblioteca go-dbf
func dbfParacsv() {
	for _, nomeArquivo := range arquivosDbc {
		caminhoDbf := "./arquivosDbc/" + strings.TrimSuffix(nomeArquivo, ".dbc") + ".dbf"
		caminhoCsv := "./arquivosDbc/" + strings.TrimSuffix(nomeArquivo, ".dbc") + ".csv"

		dbfTable, err := godbf.NewFromFile(caminhoDbf, "UTF-8")
		if err != nil {
			log.Fatal(err)
		}

		csvFile, err := os.Create(caminhoCsv)
		if err != nil {
			log.Fatal(err)
		}
		defer csvFile.Close()

		w := csv.NewWriter(csvFile)
		defer w.Flush()

		headers := make([]string, len(dbfTable.Fields()))
		for i, field := range dbfTable.Fields() {
			headers[i] = field.Name()
		}
		if err := w.Write(headers); err != nil {
			log.Fatal("Erro ao escrever cabeçalho no CSV:", err)
		}

		for i := 0; i < dbfTable.NumberOfRecords(); i++ {
			row := dbfTable.GetRowAsSlice(i)
			if err := w.Write(row); err != nil {
				log.Fatal("Erro ao escrever registro no CSV:", err)
			}
			w.Flush() // Libera buffer após cada linha
		}

		fmt.Println(nomeArquivo + ": Arquivo convertido para CSV.")
	}
}
