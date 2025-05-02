'use client';

import { useState, useEffect } from 'react';
import { User } from '@/lib/types';
import { getUsers } from '@/lib/api';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Skeleton } from '@/components/ui/skeleton';

interface UserSelectProps {
  onUserChange: (userId: string) => void;
}

export function UserSelect({ onUserChange }: UserSelectProps) {
  const [users, setUsers] = useState<User[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedUser, setSelectedUser] = useState<string>("");

  useEffect(() => {
    const fetchUsers = async () => {
      try {
        setLoading(true);
        const usersData = await getUsers();
        setUsers(usersData);
        // Automatically select first user
        if (usersData.length > 0) {
          const firstUserId = usersData[0].user_id;
          setSelectedUser(firstUserId);
          onUserChange(firstUserId);
          console.log("Initial user set to:", firstUserId);
        }
      } catch (err) {
        setError('Failed to load users');
        console.error(err);
      } finally {
        setLoading(false);
      }
    };

    fetchUsers();
  }, []); // Remove onUserChange from dependencies to prevent re-fetching

  const handleUserChange = (userId: string) => {
    console.log("User selection changed to:", userId);
    setSelectedUser(userId);
    onUserChange(userId);
  };

  if (loading) {
    return <Skeleton className="h-10 w-full" />;
  }

  if (error) {
    return <div className="text-red-500">Error: {error}</div>;
  }

  return (
    <Select value={selectedUser} onValueChange={handleUserChange}>
      <SelectTrigger className="w-full">
        <SelectValue placeholder="Select a user">
          {users.find(user => user.user_id === selectedUser)?.username || "Select a user"}
        </SelectValue>
      </SelectTrigger>
      <SelectContent>
        {users.map((user) => (
          <SelectItem key={user.user_id} value={user.user_id}>
            {user.username}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
} 