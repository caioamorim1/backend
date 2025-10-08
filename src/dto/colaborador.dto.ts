export interface CreateColaboradorDTO {
  hospitalId: string;
  nome: string;
  email: string;
  cpf: string;
  senha?: string; // se não informado, gerar padrão e forçar troca futura
  permissao: "ADMIN" | "GESTOR" | "COMUM";
}

export interface UpdateColaboradorDTO {
  nome?: string;
  email?: string;
  cpf?: string;
  permissao?: "ADMIN" | "GESTOR" | "COMUM";
}

export interface CreateAdminDTO {
  nome?: string;
  cpf?: string;
  email?: string;
  senha?: string;
}

export interface LoginAdminDTO {
  email: string;
  senha: string;
}
