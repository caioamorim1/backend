import { Router } from "express";
import { DataSource } from "typeorm";
import { HospitalSectorsRepository } from "../repositories/hospitalSectorsRepository";
import { HospitalSectorsController } from "../controllers/hospitalSectorsController";

export const HospitalSectorsRoutes = (ds: DataSource): Router => {
  const router = Router();
  const repo = new HospitalSectorsRepository(ds);
  const ctrl = new HospitalSectorsController(repo);

  // GET /hospital-sectors/:hospitalId
  router.get("/:hospitalId", ctrl.getAllSectors);

  return router;
};
