import "reflect-metadata";
import { AppDataSource } from "./ormconfig";
import { connectToDatabase } from "./database/connection";
import { createApp } from "./app";
import { scheduleSessionExpiry } from "./jobs/sessionExpiry";
import { runInitialScpMetodoSeed } from "./startup/initialSeed";
import dotenv from "dotenv";
import express from "express"; // Importe o express
import path from "path"; // Importe o 'path' do Node.js

dotenv.config({ path: ".env" });
const PORT = process.env.PORT || 3110;

(async () => {
  try {
    await connectToDatabase();
    const app = createApp(AppDataSource);

    // ✅ CORREÇÃO APLICADA AQUI
    // Configura o Express para servir arquivos estáticos da pasta 'uploads'
    // Isso torna as imagens acessíveis publicamente pela URL sem precisar de token.
    // A rota '/uploads' no navegador será mapeada para a pasta 'uploads' no seu backend.
    app.use("/uploads", express.static(path.join(__dirname, "../uploads")));

    // agenda job opcional para expirar sessões de avaliação (ocupação) automaticamente
    scheduleSessionExpiry(AppDataSource);
    // seed automático somente se faltarem métodos builtin
    await runInitialScpMetodoSeed(AppDataSource);

    const server = app.listen(PORT, () => {
      console.log(`Servidor rodando na porta ${PORT}`);
      console.log(
        `Pasta de uploads servida estaticamente em http://localhost:${PORT}/uploads`
      );
    });

    server.on("error", (err) => {
      console.error(`Falha ao iniciar servidor na porta ${PORT}:`, err);
      // encerra o processo para que orquestradores (PM2 / Docker / etc) possam reiniciar
      process.exit(1);
    });
  } catch (err) {
    console.error("Erro durante a inicialização da aplicação:", err);
    process.exit(1);
  }
})();
