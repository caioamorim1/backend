import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
  OneToOne,
  JoinColumn,
} from "typeorm";
import { Hospital } from "./Hospital";

interface SetorBaseline {
  nome: string;
  custo: string;
  ativo: boolean;
}
@Entity("baselines")
export class Baseline {
  @PrimaryGeneratedColumn("uuid")
  id!: string;

  // Owning side: store hospitalId FK on baseline table and expose relation
  @OneToOne(() => Hospital, (hospital) => hospital.baseline, {
    nullable: true,
    onDelete: "CASCADE",
  })
  @JoinColumn({ name: "hospitalId" })
  hospital!: Hospital;

  @Column({ type: "varchar", length: 255 })
  nome!: string;

  @Column({ type: "int", default: 0 })
  quantidade_funcionarios!: number;

  @Column({ type: "decimal", precision: 12, scale: 2, default: 0 })
  custo_total!: string;
  // armazenar arrays de strings no Postgres como text[] (ou use 'simple-array' se preferir uma CSV)
  @Column({ type: "text", array: true, nullable: true })
  setores!: SetorBaseline[];

  @CreateDateColumn()
  created_at!: Date;

  @UpdateDateColumn()
  updated_at!: Date;
}
