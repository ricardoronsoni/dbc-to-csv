package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/tadvi/dbf"
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
			log.Fatal(err)
		}
		if !info.IsDir() && (strings.HasSuffix(strings.ToUpper(info.Name()), ".DBF") || strings.HasSuffix(strings.ToUpper(info.Name()), ".CSV")) {
			err := os.Remove(path)
			if err != nil {
				log.Fatal(err)
			}
		}
		return nil
	})
}

// listarArquivosDbc irá listar todos os arquivos DBC no diretório ./arquivosDBC
func listarArquivosDbc() {
	filepath.Walk("./arquivosDbc", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Fatal(err)
		}
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".dbc") {
			relativePath, err := filepath.Rel("./arquivosDbc", path)
			if err != nil {
				log.Fatal(err)
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
			log.Fatal(err)
		}
		fmt.Println(nomeArquivo + ": Arquivo convertido para DBF.")
	}
}

// dbfParacsv transforma os arquivos DBF em CSV
func dbfParacsv() {
	for _, nomeArquivo := range arquivosDbc {
		csvFile, err := os.Create("./arquivosDbc/" + strings.TrimSuffix(nomeArquivo, ".dbc") + ".csv")
		if err != nil {
			log.Fatal(err)
		}
		defer csvFile.Close()

		db, err := dbf.LoadFile("./arquivosDbc/" + strings.TrimSuffix(nomeArquivo, ".dbc") + ".dbf")
		if err != nil {
			log.Fatal(err)
		}

		iter := db.NewIterator()
		for iter.Next() {
			registroDbf := iter.Row()
			w := csv.NewWriter(csvFile)
			defer w.Flush()

			if err := w.Write(registroDbf); err != nil {
				log.Fatal(err)
			}
		}
		db = nil

		fmt.Println(nomeArquivo + ": Arquivo convertido para CSV.")
	}
}
