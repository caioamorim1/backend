import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
  ManyToOne,
  OneToMany,
} from "typeorm";
import { Grupo } from "./Grupo";
import { Hospital } from "./Hospital";

@Entity()
export class Regiao {
  @PrimaryGeneratedColumn("uuid")
  id!: string;

  @Column({ type: "varchar", length: 255 })
  nome!: string;

  @CreateDateColumn({ type: "timestamp with time zone" })
  created_at!: Date;

  @UpdateDateColumn({ type: "timestamp with time zone" })
  updated_at!: Date;

  @ManyToOne(() => Grupo, (grupo: Grupo) => grupo.regioes, { nullable: false })
  grupo!: Grupo;

  @OneToMany(() => Hospital, (hospital: Hospital) => hospital.regiao)
  hospitais!: Hospital[];
}
