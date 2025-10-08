export interface CreateAdminDTO {
  nome: string;
  email: string;
  senha: string;
}

export interface LoginAdminDTO {
  email: string;
  senha: string;
}
