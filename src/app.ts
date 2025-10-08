// backend/src/app.ts
import express from "express";
import type { DataSource } from "typeorm";
import { createIndexRouter } from "./routes";
import cors from "cors";
import { authMiddleware } from "./middlewares/authMiddleware";
import path from "path"; // Importe o 'path'

// Cria e configura o app SEM efeitos colaterais (sem iniciar DB/servidor)
export function createApp(dataSource: DataSource) {
  const app = express();

  app.use(cors({ origin: true, credentials: true }));

  app.use(express.json());
  app.use(express.urlencoded({ extended: true }));

  // ✅ CORREÇÃO APLICADA AQUI
  // 1. Servir a pasta 'uploads' como estática ANTES de qualquer middleware de autenticação.
  // A rota '/uploads' no navegador será mapeada para a pasta 'uploads' no seu backend.
  app.use('/uploads', express.static(path.join(__dirname, '../uploads')));

  // 2. Proteger as rotas da API *depois* de servir os arquivos estáticos.
  app.use((req, res, next) => {
    const openPaths = [
      "/",
      "/login",
      "/admin/criar",
      "/scp-metodos/seed/builtin",
    ];
    // Se o caminho começar com /uploads, também permite (já foi tratado pelo express.static)
    if (openPaths.includes(req.path) || req.path.startsWith('/uploads')) {
      return next();
    }
    
    if (req.method === "PATCH" && req.path.endsWith("/senha")) {
      return next();
    }
    if (req.method === "OPTIONS") return next(); // CORS preflight
    
    return (authMiddleware as any)(req, res, next);
  });

  app.use("/", createIndexRouter(dataSource));

  return app;
}

export default createApp;