import { AppDataSource } from "../ormconfig";
import { DataSource } from "typeorm";

export const connectToDatabase = async (): Promise<void> => {
  try {
    await AppDataSource.initialize();
    console.log(
      "Conexão com o banco de dados PostgreSQL estabelecida com sucesso!"
    );

    // Garante que a sessão do Postgres opere em UTC (padroniza timestamps)
    try {
      await AppDataSource.query(`SET TIME ZONE 'UTC';`);
    } catch (e) {
      console.warn(
        "Aviso: não foi possível definir TIME ZONE UTC na sessão:",
        e
      );
    }

    // Adiciona constraint de exclusão para evitar períodos sobrepostos por leito
    
  } catch (error) {
    console.error(" Erro ao conectar ao banco de dados:", error);
    process.exit(1);
  }
};
