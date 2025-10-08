import { Request, Response } from "express";
import { HospitalSectorsRepository } from "../repositories/hospitalSectorsRepository";

export class HospitalSectorsController {
  constructor(private repo: HospitalSectorsRepository) {}

  getAllSectors = async (req: Request, res: Response) => {
    try {
      const { hospitalId } = req.params;

      if (!hospitalId) {
        return res.status(400).json({ error: "hospitalId é obrigatório" });
      }

      const data = await this.repo.getAllSectorsByHospital(hospitalId);
      return res.json(data);
    } catch (err) {
      return res.status(500).json({
        error: err instanceof Error ? err.message : String(err),
      });
    }
  };
}
