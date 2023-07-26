package main

import (
	"io"
	"log"
	"net/http"

	"github.com/ISSuh/lsm-tree/logging"
	"github.com/ISSuh/lsm-tree/storage"
	"github.com/gorilla/mux"
)

type StorageHandler struct {
	storage *storage.Storage
}

func newHandler(option storage.Option) *StorageHandler {
	return &StorageHandler{
		storage: storage.NewStorage(option),
	}
}

func (handler StorageHandler) Get(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	key := params["key"]

	value := handler.storage.Get(key)
	w.WriteHeader(http.StatusOK)
	w.Write(value)
}

func (handler StorageHandler) Set(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	key := params["key"]

	value, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	handler.storage.Set(key, value)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte{})
}

func (handler StorageHandler) Remove(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	key := params["key"]

	handler.storage.Remove(key)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte{})
}

func main() {
	logging.Info("storage server")
	storageOption := storage.NewOption()
	storageOption.Path = "./temp"
	storageHandler := newHandler(storageOption)

	router := mux.NewRouter()
	router.HandleFunc("/api/{key}", storageHandler.Get).Methods("GET")
	router.HandleFunc("/api/{key}", storageHandler.Set).Methods("POST")
	router.HandleFunc("/api/{key}", storageHandler.Remove).Methods("DELETE")

	http.Handle("/", router)
	err := http.ListenAndServe(":33660", nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
