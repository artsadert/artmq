package user

import (
	"fmt"

	"github.com/google/uuid"
)

type User struct {
	Id       uuid.UUID
	Username string
}

func NewUser(username string) (*User, error) {
	if username == "" {
		return nil, fmt.Errorf("username is required")
	}

	return &User{
		Id:       uuid.New(),
		Username: username,
	}, nil
}
