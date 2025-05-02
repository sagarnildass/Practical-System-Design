'use client';

import { useState, useEffect } from 'react';
import { User, UserStats } from '@/lib/types';
import { getUserById, getUserStats } from '@/lib/api';
import { Card, CardContent, CardHeader } from '@/components/ui/card';
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar';
import { Skeleton } from '@/components/ui/skeleton';
import { Button } from '@/components/ui/button';

interface UserProfileProps {
  userId: string;
  onFollowClick?: () => void;
}

export function UserProfile({ userId, onFollowClick }: UserProfileProps) {
  const [user, setUser] = useState<User | null>(null);
  const [stats, setStats] = useState<UserStats | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchUserData = async () => {
      if (!userId) return;

      console.log("UserProfile: Loading data for user ID:", userId);
      try {
        setLoading(true);
        const userData = await getUserById(userId);
        setUser(userData);

        try {
          const userStats = await getUserStats(userId);
          setStats(userStats);
        } catch (statsErr) {
          console.error('Failed to load user stats:', statsErr);
          // Continue even if stats fail
          setStats({
            posts_count: 0,
            followers_count: 0,
            following_count: 0
          });
        }
      } catch (err) {
        setError('Failed to load user');
        console.error(err);
      } finally {
        setLoading(false);
      }
    };

    fetchUserData();
  }, [userId]);

  if (loading) {
    return (
      <Card>
        <CardHeader className="pb-0">
          <div className="flex items-center space-x-4">
            <Skeleton className="h-12 w-12 rounded-full" />
            <div className="space-y-2">
              <Skeleton className="h-4 w-[200px]" />
              <Skeleton className="h-4 w-[150px]" />
            </div>
          </div>
        </CardHeader>
        <CardContent>
          <div className="flex justify-between pt-4">
            <Skeleton className="h-4 w-[80px]" />
            <Skeleton className="h-4 w-[80px]" />
            <Skeleton className="h-4 w-[80px]" />
          </div>
        </CardContent>
      </Card>
    );
  }

  if (error || !user) {
    return <div className="text-red-500">Error: {error || 'User not found'}</div>;
  }

  return (
    <Card>
      <CardHeader className="pb-0">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-4">
            <Avatar className="h-12 w-12">
              <AvatarImage src={user.profile_picture_url} alt={user.username} />
              <AvatarFallback>{user.username.substring(0, 2).toUpperCase()}</AvatarFallback>
            </Avatar>
            <div>
              <h2 className="text-xl font-bold">{user.username}</h2>
              <p className="text-sm text-gray-500">{user.email}</p>
            </div>
          </div>
          {onFollowClick && (
            <Button onClick={onFollowClick} variant="outline" size="sm">
              Follow
            </Button>
          )}
        </div>
      </CardHeader>
      <CardContent>
        <div className="flex justify-between pt-4 text-center">
          <div>
            <div className="text-xl font-bold">{stats?.posts_count || 0}</div>
            <div className="text-xs text-gray-500">Posts</div>
          </div>
          <div>
            <div className="text-xl font-bold">{stats?.followers_count || 0}</div>
            <div className="text-xs text-gray-500">Followers</div>
          </div>
          <div>
            <div className="text-xl font-bold">{stats?.following_count || 0}</div>
            <div className="text-xs text-gray-500">Following</div>
          </div>
        </div>
      </CardContent>
    </Card>
  );
} 