export interface StaffDTO {
  id: string;
  role: string;
  quantity: number;
}

export interface CareLevelDTO {
  minimumCare: number;
  intermediateCare: number;
  highDependency: number;
  semiIntensive: number;
  intensive: number;
}

export interface BedStatusDTO {
  evaluated: number;
  vacant: number;
  inactive: number;
}

export interface InternationSectorDTO {
  id: string;
  name: string;
  descr: string | null;
  costAmount: number;
  bedCount: number;
  careLevel: CareLevelDTO;
  bedStatus: BedStatusDTO;
  staff: StaffDTO[];
}

export interface AssistanceSectorDTO {
  id: string;
  name: string;
  descr: string | null;
  costAmount: number;
  staff: StaffDTO[];
}

export interface HospitalSectorsDTO {
  id: string;
  internation: InternationSectorDTO[];
  assistance: AssistanceSectorDTO[];
}
