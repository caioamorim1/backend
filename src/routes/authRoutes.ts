import { Router } from "express";
import { DataSource } from "typeorm";
import { AuthService } from "../services/authService";
import { AuthController } from "../controllers/authController";

export const AuthRoutes = (ds: DataSource): Router => {
  const r = Router();
  const auth = new AuthService(ds);
  const ctrl = new AuthController(auth);

  // Route: POST /login - realiza login e retorna token
  r.post("/", ctrl.login); // POST /login

  return r;
};
